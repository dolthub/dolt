// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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
		util.CheckErrorNoUsage(fmt.Errorf("incorrect number of arguments"))
	}
	db, err := cfg.GetDatabase(ctx, args[0])
	util.CheckError(err)
	defer db.Close()

	leftDS, rightDS, outDS, err := resolveDatasets(ctx, db, args[1], args[2], args[3])

	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	left, right, ancestor, err := getMergeCandidates(ctx, db, leftDS, rightDS)

	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}

	policy := decidePolicy(resolver)
	pc := newMergeProgressChan()
	merged, err := policy(ctx, left, right, ancestor, db, pc)
	util.CheckErrorNoUsage(err)
	close(pc)

	leftHeadRef, ok := leftDS.MaybeHeadRef()

	if !ok {
		fmt.Fprintln(os.Stderr, args[1]+" has no head value.")
		return 1
	}

	rightHeadRef, ok := rightDS.MaybeHeadRef()

	if !ok {
		fmt.Fprintln(os.Stderr, args[2]+" has no head value.")
		return 1
	}

	_, err = db.SetHead(ctx, outDS, db.WriteValue(ctx, datas.NewCommit(merged, types.NewSet(ctx, db, leftHeadRef, rightHeadRef), types.EmptyStruct(db.Format()))))
	d.PanicIfError(err)

	if !verbose.Quiet() {
		status.Printf("Done")
		status.Done()
	}
	return 0
}

func resolveDatasets(ctx context.Context, db datas.Database, leftName, rightName, outName string) (leftDS, rightDS, outDS datas.Dataset, err error) {
	makeDS := func(dsName string) (datas.Dataset, error) {
		if !datasetRe.MatchString(dsName) {
			util.CheckErrorNoUsage(fmt.Errorf("Invalid dataset %s, must match %s", dsName, datas.DatasetRe.String()))
		}
		return db.GetDataset(ctx, dsName)
	}

	leftDS, err = makeDS(leftName)

	if err != nil {
		return datas.Dataset{}, datas.Dataset{}, datas.Dataset{}, err
	}

	rightDS, err = makeDS(rightName)

	if err != nil {
		return datas.Dataset{}, datas.Dataset{}, datas.Dataset{}, err
	}

	outDS, err = makeDS(outName)

	if err != nil {
		return datas.Dataset{}, datas.Dataset{}, datas.Dataset{}, err
	}

	return
}

func getMergeCandidates(ctx context.Context, db datas.Database, leftDS, rightDS datas.Dataset) (left, right, ancestor types.Value, err error) {
	leftRef, ok := leftDS.MaybeHeadRef()
	checkIfTrue(!ok, "Dataset %s has no data", leftDS.ID())
	rightRef, ok := rightDS.MaybeHeadRef()
	checkIfTrue(!ok, "Dataset %s has no data", rightDS.ID())
	ancestorCommit, ok := getCommonAncestor(ctx, leftRef, rightRef, db)
	checkIfTrue(!ok, "Datasets %s and %s have no common ancestor", leftDS.ID(), rightDS.ID())

	leftHead, ok := leftDS.MaybeHeadValue()

	if !ok {
		return nil, nil, nil, err
	}

	rightHead, ok := rightDS.MaybeHeadValue()

	if !ok {
		return nil, nil, nil, err
	}

	return leftHead, rightHead, ancestorCommit.Get(datas.ValueField), nil

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
		panic("Not a commit: " + types.EncodedValueMaxLines(ctx, v, 10) + "  ...")
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
	fmt.Fprintf(out, "\nConflict at: %s\n", path.String())
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
