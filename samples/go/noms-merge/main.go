// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/merge"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var datasetRe = regexp.MustCompile("^" + datas.DatasetRe.String() + "$")

func main() {
	if err := nomsMerge(); err != nil {
		fmt.Println(err)
		exit.Fail()
	}
}

func nomsMerge() error {
	outDSStr := flag.String("out-ds-name", "", "output dataset to write to - if empty, defaults to <right-ds-name>")
	parentStr := flag.String("parent", "", "common ancestor of <left-ds-name> and <right-ds-name> (currently required; soon to be optional)")
	quiet := flag.Bool("quiet", false, "silence progress output")
	verbose.RegisterVerboseFlags(flag.CommandLine)
	flag.Usage = usage

	return d.Unwrap(d.Try(func() {
		flag.Parse(false)

		if flag.NArg() == 0 {
			flag.Usage()
			d.PanicIfTrue(true, "")
		}

		d.PanicIfTrue(flag.NArg() != 3, "Incorrect number of arguments\n")
		d.PanicIfTrue(*parentStr == "", "--parent is required\n")

		cfg := config.NewResolver()
		db, err := cfg.GetDatabase(flag.Arg(0))
		defer db.Close()
		d.PanicIfError(err)

		makeDS := func(dsName string) datas.Dataset {
			d.PanicIfTrue(!datasetRe.MatchString(dsName), "Invalid dataset %s, must match %s\n", dsName, datas.DatasetRe.String())
			return db.GetDataset(dsName)
		}

		leftDS := makeDS(flag.Arg(1))
		rightDS := makeDS(flag.Arg(2))
		parentDS := makeDS(*parentStr)

		parent, ok := parentDS.MaybeHeadValue()
		d.PanicIfTrue(!ok, "Dataset %s has no data\n", *parentStr)
		left, ok := leftDS.MaybeHeadValue()
		d.PanicIfTrue(!ok, "Dataset %s has no data\n", flag.Arg(1))
		right, ok := rightDS.MaybeHeadValue()
		d.PanicIfTrue(!ok, "Dataset %s has no data\n", flag.Arg(2))

		outDS := rightDS
		if *outDSStr != "" {
			outDS = makeDS(*outDSStr)
		}

		pc := make(chan struct{}, 128)
		go func() {
			count := 0
			for range pc {
				if !*quiet {
					count++
					status.Printf("Applied %d changes...", count)
				}
			}
		}()
		resolve := func(aType, bType types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
			return cliResolve(os.Stdin, os.Stdout, aType, bType, a, b, path)
		}
		merged, err := merge.ThreeWay(left, right, parent, db, resolve, pc)
		d.PanicIfError(err)

		_, err = db.Commit(outDS, merged, datas.CommitOptions{
			Parents: types.NewSet(leftDS.HeadRef(), rightDS.HeadRef()),
			Meta:    parentDS.Head().Get(datas.MetaField).(types.Struct),
		})
		d.PanicIfError(err)
		if !*quiet {
			status.Printf("Done")
			status.Done()
		}
	}))
}

func cliResolve(in io.Reader, out io.Writer, aType, bType types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	stringer := func(v types.Value) (s string, success bool) {
		switch v := v.(type) {
		case types.Bool, types.Number, types.String:
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
		fmt.Fprintln(out, "Enter 'l' to accept the left value, 'r' to accept the right value, or 'm' to mash them together")
		_, err := fmt.Fscanf(in, "%c\n", &choice)
		d.PanicIfError(err)
		switch choice {
		case 'l', 'L':
			return aType, a, true
		case 'r', 'R':
			return bType, b, true
		case 'm', 'M':
			if !a.Type().Equals(b.Type()) {
				fmt.Fprintf(out, "Sorry, can't merge a %s with a %s\n", a.Type().Describe(), b.Type().Describe())
				return change, merged, false
			}
			switch a := a.(type) {
			case types.Bool:
				merged = types.Bool(bool(a) || bool(b.(types.Bool)))
			case types.Number:
				merged = types.Number(float64(a) + float64(b.(types.Number)))
			case types.String:
				merged = types.String(string(a) + string(b.(types.String)))
			}
			fmt.Fprintln(out, "Replacing with", types.EncodedValue(merged))
			return aType, merged, true
		}
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Attempts to merge the two datasets in the provided database and commit the merge to either <right-ds-name> or another dataset of your choice.\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s [--out-ds-name=<name>] [--parent=<name>] <db-spec> <left-ds-name> <right-ds-name>\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  <db-spec>       : database in which named datasets live\n")
	fmt.Fprintf(os.Stderr, "  <left-ds-name>  : name of a dataset descending from <parent>\n")
	fmt.Fprintf(os.Stderr, "  <right-ds-name> : name of another dataset descending from <parent>\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n\n")
	flag.PrintDefaults()
}
