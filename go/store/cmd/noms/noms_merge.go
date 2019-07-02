// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/config"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/merge"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/status"
	"github.com/liquidata-inc/ld/dolt/go/store/util/verbose"
)

var (
	resolver string

	nomsMerge = &util.Command{
		Run:       runMerge,
		UsageLine: "merge [options] <database> <left-dataset-name> <right-dataset-name> <output-dataset-name>",
		Short:     "Merges and commits the head values of two named datasets",
		Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.\nYu must provide a working database and the names of two Datasets you want to merge. The values at the heads of these Datasets will be merged, put into a new Commit object, and set as the Head of the third provided Dataset name.",
		Flags:     setupMergeFlags,
		Nargs:     1, // if absolute-path not present we read it from stdin
	}
	datasetRe = regexp.MustCompile("^" + datas.DatasetRe.String() + "$")
)

func setupMergeFlags() *flag.FlagSet {
	commitFlagSet := flag.NewFlagSet("merge", flag.ExitOnError)
	commitFlagSet.StringVar(&resolver, "policy", "n", "conflict resolution policy for merging. Defaults to 'n', which means no resolution strategy will be applied. Supported values are 'l' (left), 'r' (right) and 'p' (prompt). 'prompt' will bring up a simple command-line prompt allowing you to resolve conflicts by choosing between 'l' or 'r' on a case-by-case basis.")
	verbose.RegisterVerboseFlags(commitFlagSet)
	return commitFlagSet
}

func checkIfTrue(b bool, format string, args ...interface{}) {
	if b {
		util.CheckErrorNoUsage(fmt.Errorf(format, args...))
	}
}

func runMerge(ctx context.Context, args []string) int {
	cfg := config.NewResolver()

	if len(args) != 4 {
		util.CheckErrorNoUsage(fmt.Errorf("Incorrect number of arguments"))
	}
	db, err := cfg.GetDatabase(ctx, args[0])
	util.CheckError(err)
	defer db.Close()

	leftDS, rightDS, outDS := resolveDatasets(ctx, db, args[1], args[2], args[3])
	left, right, ancestor := getMergeCandidates(ctx, db, leftDS, rightDS)
	policy := decidePolicy(resolver)
	pc := newMergeProgressChan()
	merged, err := policy(ctx, left, right, ancestor, db, pc)
	util.CheckErrorNoUsage(err)
	close(pc)

	_, err = db.SetHead(ctx, outDS, db.WriteValue(ctx, datas.NewCommit(merged, types.NewSet(ctx, types.Format_7_18, db, leftDS.HeadRef(), rightDS.HeadRef()), types.EmptyStruct(types.Format_7_18))))
	d.PanicIfError(err)
	if !verbose.Quiet() {
		status.Printf("Done")
		status.Done()
	}
	return 0
}

func resolveDatasets(ctx context.Context, db datas.Database, leftName, rightName, outName string) (leftDS, rightDS, outDS datas.Dataset) {
	makeDS := func(dsName string) datas.Dataset {
		if !datasetRe.MatchString(dsName) {
			util.CheckErrorNoUsage(fmt.Errorf("Invalid dataset %s, must match %s", dsName, datas.DatasetRe.String()))
		}
		return db.GetDataset(ctx, dsName)
	}
	leftDS = makeDS(leftName)
	rightDS = makeDS(rightName)
	outDS = makeDS(outName)
	return
}

func getMergeCandidates(ctx context.Context, db datas.Database, leftDS, rightDS datas.Dataset) (left, right, ancestor types.Value) {
	leftRef, ok := leftDS.MaybeHeadRef()
	checkIfTrue(!ok, "Dataset %s has no data", leftDS.ID())
	rightRef, ok := rightDS.MaybeHeadRef()
	checkIfTrue(!ok, "Dataset %s has no data", rightDS.ID())
	ancestorCommit, ok := getCommonAncestor(ctx, leftRef, rightRef, db)
	checkIfTrue(!ok, "Datasets %s and %s have no common ancestor", leftDS.ID(), rightDS.ID())

	return leftDS.HeadValue(), rightDS.HeadValue(), ancestorCommit.Get(datas.ValueField)
}

func getCommonAncestor(ctx context.Context, r1, r2 types.Ref, vr types.ValueReader) (a types.Struct, found bool) {
	aRef, found := datas.FindCommonAncestor(ctx, r1, r2, vr)
	if !found {
		return
	}
	v := vr.ReadValue(ctx, aRef.TargetHash())
	if v == nil {
		panic(aRef.TargetHash().String() + " not found")
	}
	if !datas.IsCommit(v) {
		panic("Not a commit: " + types.EncodedValueMaxLines(ctx, types.Format_7_18, v, 10) + "  ...")
	}
	return v.(types.Struct), true
}

func newMergeProgressChan() chan struct{} {
	pc := make(chan struct{}, 128)
	go func() {
		count := 0
		for range pc {
			if !verbose.Quiet() {
				count++
				status.Printf("Applied %d changes...", count)
			}
		}
	}()
	return pc
}

func decidePolicy(policy string) merge.Policy {
	var resolve merge.ResolveFunc
	switch policy {
	case "n", "N":
		resolve = merge.None
	case "l", "L":
		resolve = merge.Ours
	case "r", "R":
		resolve = merge.Theirs
	case "p", "P":
		resolve = func(aType, bType types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
			return cliResolve(os.Stdin, os.Stdout, aType, bType, a, b, path)
		}
	default:
		util.CheckErrorNoUsage(fmt.Errorf("Unsupported merge policy: %s. Choices are n, l, r and a.", policy))
	}
	return merge.NewThreeWay(resolve)
}

func cliResolve(in io.Reader, out io.Writer, aType, bType types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	stringer := func(v types.Value) (s string, success bool) {
		switch v := v.(type) {
		case types.Bool, types.Float, types.String:
			return fmt.Sprintf("%v", v), true
		}
		return "", false
	}
	left, lOk := stringer(a)
	right, rOk := stringer(b)
	if !lOk || !rOk {
		return change, merged, false
	}

	// TODO: Handle removes as well.
	fmt.Fprintf(out, "\nConflict at: %s\n", path.String(types.Format_7_18))
	fmt.Fprintf(out, "Left:  %s\nRight: %s\n\n", left, right)
	var choice rune
	for {
		fmt.Fprintln(out, "Enter 'l' to accept the left value, 'r' to accept the right value")
		_, err := fmt.Fscanf(in, "%c\n", &choice)
		d.PanicIfError(err)
		switch choice {
		case 'l', 'L':
			return aType, a, true
		case 'r', 'R':
			return bType, b, true
		}
	}
}
