// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"path"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/walk"
	flag "github.com/juju/gnuflag"
)

func main() {
	if !index() {
		os.Exit(1)
	}
}

func index() (win bool) {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to - if empty, defaults to input dataset")
	var parallelism = flag.Int("parallelism", 16, "number of parallel goroutines to search")

	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "Need at least one dataset to index")
		return
	}

	db, err := spec.GetDatabase(*dbStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input database '%s': %s\n", flag.Arg(0), err)
		return
	}
	defer db.Close()

	var outDS datas.Dataset
	if !datas.DatasetFullRe.MatchString(*outDSStr) {
		fmt.Fprintf(os.Stderr, "Invalid output dataset name: %s\n", *outDSStr)
		return
	} else {
		outDS = db.GetDataset(*outDSStr)
	}

	inputs := []types.Value{}
	for i := 0; i < flag.NArg(); i++ {
		p, err := spec.NewAbsolutePath(flag.Arg(i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid input path '%s', error: %s\n", flag.Arg(i), err)
			return
		}

		v := p.Resolve(db)
		if v == nil {
			fmt.Fprintf(os.Stderr, "Input path '%s' does not exist in '%s'", flag.Arg(i), *dbStr)
			return
		}

		inputs = append(inputs, v)
		continue
	}

	sizeType := types.MakeStructTypeFromFields("", types.FieldMap{
		"width":  types.NumberType,
		"height": types.NumberType,
	})
	dateType := types.MakeStructTypeFromFields("Date", types.FieldMap{
		"nsSinceEpoch": types.NumberType,
	})
	fields := types.FieldMap{
		"sizes":         types.MakeMapType(sizeType, types.StringType),
		"tags":          types.MakeSetType(types.StringType),
		"title":         types.StringType,
		"datePublished": dateType,
		"dateUpdated":   dateType,
	}
	photoType := types.MakeStructTypeFromFields("Photo", fields)
	fields["dateTaken"] = dateType
	photoType = types.MakeUnionType(photoType, types.MakeStructTypeFromFields("Photo", fields))

	byDate := types.NewGraphBuilder(db, types.MapKind, true)
	byTag := types.NewGraphBuilder(db, types.MapKind, true)

	for _, v := range inputs {
		walk.SomeP(v, db, func(cv types.Value, _ *types.Ref) (stop bool) {
			if types.IsSubtype(photoType, cv.Type()) {
				s := cv.(types.Struct)
				// Prefer to sort by the actual date the photo was taken, but if it's not
				// available, use the date it was published instead.
				ds, ok := s.MaybeGet("dateTaken")
				if !ok {
					ds = s.Get("datePublished")
				}

				// Sort by most recent by negating the timestamp.
				d := ds.(types.Struct).Get("nsSinceEpoch").(types.Number)
				d = types.Number(-float64(d))

				byDate.SetInsert([]types.Value{d}, cv)
				s.Get("tags").(types.Set).IterAll(func(t types.Value) {
					byTag.SetInsert([]types.Value{t, d}, cv)
				})
				// Can't be any photos inside photos, so we can save a little bit here.
				stop = true
			}
			return
		}, *parallelism)
	}

	outDS, err = db.CommitValue(outDS, types.NewStruct("", types.StructData{
		"byDate": byDate.Build(),
		"byTag":  byTag.Build(),
	}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not commit: %s\n", err)
		return
	}

	win = true
	return
}

func usage() {
	fmt.Fprintf(os.Stderr, "photo-index indexes photos by common attributes\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s -db=<db-spec> -out-ds=<name> [input-paths...]\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "  <db>             : Database to work with\n")
	fmt.Fprintf(os.Stderr, "  <out-ds>         : Dataset to write index to\n")
	fmt.Fprintf(os.Stderr, "  [input-paths...] : One or more paths within <db-spec> to crawl\n\n")
	fmt.Fprintln(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}
