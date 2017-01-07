// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"math"
	"os"
	"path"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/exit"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/go/walk"
	flag "github.com/juju/gnuflag"
)

func main() {
	if !index() {
		exit.Fail()
	}
}

type Photo struct {
	Id            string
	Sizes         map[struct{ Width, Height int }]string
	DateTaken     Date         `noms:",omitempty"`
	DatePublished Date         `noms:",omitempty"`
	DateUpdated   Date         `noms:",omitempty"`
	Tags          []string     `noms:",omitempty"`
	Sources       []string     `noms:",omitempty"`
	Original      types.Struct `noms:",original"`
	Faces         []struct {
		Name       string
		X, Y, W, H float32
	} `noms:",omitempty"`
}

type PhotoGroup struct {
	Id       string
	Cover    Photo
	Photos   []Photo
	Original types.Struct `noms:",original"`
}

type Date struct {
	NsSinceEpoch float64
}

func (d Date) IsEmpty() bool {
	return d.NsSinceEpoch == 0
}

func index() (win bool) {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to - if empty, defaults to input dataset")
	var indexCovers = flag.Bool("index-covers", false, "the resulting index will contain only the cover Photo, not the entire PhotoGroup")
	verbose.RegisterVerboseFlags(flag.CommandLine)

	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	cfg := config.NewResolver()
	db, err := cfg.GetDatabase(*dbStr)
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
	d.CheckErrorNoUsage(err)

	byDate := types.NewGraphBuilder(db, types.MapKind, true)
	byFace := types.NewGraphBuilder(db, types.MapKind, true)
	bySource := types.NewGraphBuilder(db, types.MapKind, true)
	byTag := types.NewGraphBuilder(db, types.MapKind, true)

	faceCounts := map[types.String]int{}
	sourceCounts := map[types.String]int{}
	tagCounts := map[types.String]int{}

	addToIndex := func(gb *types.GraphBuilder, path []types.Value, pg PhotoGroup) {
		if *indexCovers {
			gb.SetInsert(path, pg.Cover.Original)
		} else {
			gb.SetInsert(path, pg.Original)
		}
	}

	addToIndexes := func(pg PhotoGroup) {
		d := math.MaxFloat64
		if !pg.Cover.DateTaken.IsEmpty() {
			d = pg.Cover.DateTaken.NsSinceEpoch
		} else if !pg.Cover.DatePublished.IsEmpty() {
			d = pg.Cover.DatePublished.NsSinceEpoch
		} else if !pg.Cover.DateUpdated.IsEmpty() {
			d = pg.Cover.DateUpdated.NsSinceEpoch
		}
		d = -d

		// Index by date
		addToIndex(byDate, []types.Value{types.Number(d)}, pg)

		allPhotos := []Photo{pg.Cover}
		if !*indexCovers {
			allPhotos = append(allPhotos, pg.Photos...)
		}

		// Index by tag, then date
		for _, p := range allPhotos {
			for _, t := range p.Tags {
				addToIndex(byTag, []types.Value{types.String(t), types.Number(d)}, pg)
				tagCounts[types.String(t)]++
			}
		}

		// Index by face, then date
		for _, p := range allPhotos {
			for _, f := range p.Faces {
				addToIndex(byFace, []types.Value{types.String(f.Name), types.Number(d)}, pg)
				faceCounts[types.String(f.Name)]++
			}
		}

		// Index by source, then date
		for _, p := range allPhotos {
			for _, s := range p.Sources {
				addToIndex(bySource, []types.Value{types.String(s), types.Number(d)}, pg)
				sourceCounts[types.String(s)]++
			}
		}
	}

	for _, v := range inputs {
		walk.WalkValues(v, db, func(cv types.Value) (stop bool) {
			var pg PhotoGroup
			if err := marshal.Unmarshal(cv, &pg); err == nil {
				stop = true
				addToIndexes(pg)
			}
			return
		})
	}

	outDS, err = db.Commit(outDS, types.NewStruct("", types.StructData{
		"byDate":         byDate.Build(),
		"byFace":         byFace.Build(),
		"bySource":       bySource.Build(),
		"byTag":          byTag.Build(),
		"facesByCount":   stringsByCount(db, faceCounts),
		"sourcesByCount": stringsByCount(db, sourceCounts),
		"tagsByCount":    stringsByCount(db, tagCounts),
	}), datas.CommitOptions{
		Meta: types.NewStruct("", types.StructData{
			"date": types.String(time.Now().Format(time.RFC3339)),
		}),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not commit: %s\n", err)
		return
	}

	win = true
	return
}

func stringsByCount(db datas.Database, strings map[types.String]int) types.Map {
	b := types.NewGraphBuilder(db, types.MapKind, true)
	for s, count := range strings {
		// Sort by largest count by negating.
		b.SetInsert([]types.Value{types.Number(-count)}, s)
	}
	return b.Build().(types.Map)
}

func usage() {
	fmt.Fprintf(os.Stderr, "photo-index indexes photos by common attributes\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] [input-paths...]\n\n", path.Base(os.Args[0]))
	fmt.Fprintf(os.Stderr, "  [input-paths...] : One or more paths within database to crawl for photos\n\n")
	fmt.Fprintln(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}
