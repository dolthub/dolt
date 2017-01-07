// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/go/walk"
	flag "github.com/juju/gnuflag"
)

type facePoints struct {
	x    types.Number
	y    types.Number
	w    types.Number
	h    types.Number
	cx   types.Number
	cy   types.Number
	name types.String
}

func main() {
	mergeFaces()
}

func mergeFaces() {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to")
	verbose.RegisterVerboseFlags(flag.CommandLine)

	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	db, err := config.NewResolver().GetDatabase(*dbStr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input database '%s': %s\n", flag.Arg(0), err)
		return
	}

	defer db.Close()
	if !datas.IsValidDatasetName(*outDSStr) {
		fmt.Fprintf(os.Stderr, "Invalid output dataset name: %s\n", *outDSStr)
		return
	}

	outDS := db.GetDataset(*outDSStr)
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

	faceRectType := types.MakeStructTypeFromFields("", types.FieldMap{
		"x": types.NumberType,
		"y": types.NumberType,
		"w": types.NumberType,
		"h": types.NumberType,
	})

	faceCenterType := types.MakeStructTypeFromFields("", types.FieldMap{
		"x":    types.NumberType,
		"y":    types.NumberType,
		"name": types.StringType,
	})

	withFaceType := types.MakeStructTypeFromFields("Photo", types.FieldMap{
		"facesRect":     types.MakeSetType(faceRectType),
		"facesCentered": types.MakeSetType(faceCenterType),
	})

	photoType := types.MakeStructTypeFromFields("Photo", types.FieldMap{
		"sizes":         types.MakeMapType(sizeType, types.StringType),
		"title":         types.StringType,
		"datePublished": dateType,
		"dateUpdated":   dateType,
	})

	annotatedPhotoSet := types.NewGraphBuilder(db, types.SetKind, true)

	for _, v := range inputs {
		walk.WalkValues(v, db, func(cv types.Value) bool {
			if types.IsSubtype(photoType, cv.Type()) {
				photo := cv.(types.Struct)
				if types.IsSubtype(withFaceType, photo.Type()) {
					photo = photo.Set("faces", getMergedFaces(photo))
				} else {
					photo.Set("faces", types.NewSet())
				}
				photo = filterExtraFields(photo)
				annotatedPhotoSet.SetInsert(nil, photo)
			}
			return false
		})
	}

	outDS, err = db.Commit(outDS, annotatedPhotoSet.Build(), datas.CommitOptions{
		Meta: types.NewStruct("", types.StructData{
			"date": types.String(time.Now().Format(time.RFC3339)),
		}),
	})
}

func filterExtraFields(photo types.Struct) types.Struct {
	filteredPhoto := types.NewStruct("Photo", types.StructData{})
	photo.Type().Desc.(types.StructDesc).IterFields(func(fieldName string, t *types.Type) {
		if fieldName != "facesCentered" && fieldName != "facesRect" {
			filteredPhoto = filteredPhoto.Set(fieldName, photo.Get(fieldName))
		}
	})
	return filteredPhoto
}

func getMergedFaces(photo types.Struct) types.Set {
	facesRect := photo.Get("facesRect")
	facesCentered := photo.Get("facesCentered")
	faceSet := types.NewSet()

	facesCentered.(types.Set).Iter(func(faceCentered types.Value) bool {
		facesRect.(types.Set).Iter(func(faceRect types.Value) (stop bool) {
			facePts := getFacePoints(faceRect.(types.Struct), faceCentered.(types.Struct))
			if centeredFaceAligned(facePts) {
				faceSet = faceSet.Insert(types.NewStruct("", types.StructData{
					"x":    facePts.x,
					"y":    facePts.y,
					"w":    facePts.w,
					"h":    facePts.h,
					"name": facePts.name,
				}))
				stop = true
			}
			return
		})
		return false
	})
	return faceSet
}

func getFacePoints(faceRect, faceCenter types.Struct) facePoints {
	x := faceRect.Get("x").(types.Number)
	y := faceRect.Get("y").(types.Number)
	h := faceRect.Get("h").(types.Number)
	w := faceRect.Get("w").(types.Number)
	cx := faceCenter.Get("x").(types.Number)
	cy := faceCenter.Get("y").(types.Number)
	name := faceCenter.Get("name").(types.String)
	return facePoints{x, y, h, w, cx, cy, name}
}

func centeredFaceAligned(facePts facePoints) bool {
	return facePts.cx >= facePts.x && facePts.cx <= facePts.x+facePts.w && facePts.cy >= facePts.y && facePts.y <= facePts.y+facePts.h
}

func usage() {
	fmt.Fprintf(os.Stderr, "face-merge merges face photo data to create comprehensive face view\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s -db=<db-spec> -out-ds=<name> [input-paths...]\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "  <db>             : Database to work with\n")
	fmt.Fprintf(os.Stderr, "  <out-ds>         : Dataset to write to\n")
	fmt.Fprintf(os.Stderr, "  [input-path] : input paths within <db-spec> to crawl\n\n")
	fmt.Fprintln(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}
