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
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/noms/go/walk"
	flag "github.com/juju/gnuflag"
)

func main() {
	if !index() {
		exit.Fail()
	}
}

func index() (win bool) {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to - if empty, defaults to input dataset")

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
	if !datas.IsValidDatasetName(*outDSStr) {
		fmt.Fprintf(os.Stderr, "Invalid output dataset name: %s\n", *outDSStr)
		return
	} else {
		outDS = db.GetDataset(*outDSStr)
	}

	inputs, err := spec.ReadAbsolutePaths(db, flag.Args()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	sizeType := types.MakeStructTypeFromFields("", types.FieldMap{
		"width":  types.NumberType,
		"height": types.NumberType,
	})
	dateType := types.MakeStructTypeFromFields("Date", types.FieldMap{
		"nsSinceEpoch": types.NumberType,
	})
	faceType := types.MakeStructTypeFromFields("", types.FieldMap{
		"name": types.StringType,
		"x":    types.NumberType,
		"y":    types.NumberType,
		"w":    types.NumberType,
		"h":    types.NumberType,
	})
	photoType := types.MakeStructTypeFromFields("Photo", types.FieldMap{
		"sizes":         types.MakeMapType(sizeType, types.StringType),
		"tags":          types.MakeSetType(types.StringType),
		"title":         types.StringType,
		"datePublished": dateType,
		"dateUpdated":   dateType,
	})

	withDateTaken := types.MakeStructTypeFromFields("", types.FieldMap{
		"dateTaken": dateType,
	})
	withFaces := types.MakeStructTypeFromFields("", types.FieldMap{
		"faces": types.MakeSetType(faceType),
	})

	byDate := types.NewGraphBuilder(db, types.MapKind, true)
	byTag := types.NewGraphBuilder(db, types.MapKind, true)
	byFace := types.NewGraphBuilder(db, types.MapKind, true)

	for _, v := range inputs {
		walk.WalkValues(v, db, func(cv types.Value) (stop bool) {
			if types.IsSubtype(photoType, cv.Type()) {
				s := cv.(types.Struct)

				// Prefer to sort by the actual date the photo was taken, but if it's not
				// available, use the date it was published instead.
				ds := s.Get("datePublished")
				if types.IsSubtype(withDateTaken, cv.Type()) {
					ds = s.Get("dateTaken")
				}

				// Sort by most recent by negating the timestamp.
				d := ds.(types.Struct).Get("nsSinceEpoch").(types.Number)
				d = types.Number(-float64(d))

				// Index by date
				byDate.SetInsert([]types.Value{d}, cv)

				// Index by tag, then date
				s.Get("tags").(types.Set).IterAll(func(t types.Value) {
					byTag.SetInsert([]types.Value{t, d}, cv)
				})

				// Index by face, then date
				if types.IsSubtype(withFaces, cv.Type()) {
					s.Get("faces").(types.Set).IterAll(func(t types.Value) {
						byFace.SetInsert([]types.Value{t.(types.Struct).Get("name"), d}, cv)
					})
				}

				// Can't be any photos inside photos, so we can save a little bit here.
				stop = true
			}
			return
		})
	}

	outDS, err = db.CommitValue(outDS, types.NewStruct("", types.StructData{
		"byDate": byDate.Build(),
		"byTag":  byTag.Build(),
		"byFace": byFace.Build(),
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
