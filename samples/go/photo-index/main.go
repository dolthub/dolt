// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
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
	Id    string
	Sizes map[struct{ Width, Height int }]string
}

type PhotoGroup struct {
	Cover  Photo
	Photos types.Set
}

type Date struct {
	NsSinceEpoch float64
}

func index() (win bool) {
	var dbStr = flag.String("db", "", "input database spec")
	var groupsStr = flag.String("groups", "", "path within db to look for PhotoGroup structs")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to - if empty, defaults to input dataset")
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
	countsMtx := sync.Mutex{}

	addToIndex := func(p Photo, cv types.Value) {
		d := math.MaxFloat64
		var dt struct{ DateTaken Date }
		var dp struct{ DatePublished Date }
		var du struct{ DateUpdated Date }
		if err := marshal.Unmarshal(cv, &dt); err == nil {
			d = -dt.DateTaken.NsSinceEpoch
		} else if err := marshal.Unmarshal(cv, &dp); err == nil {
			d = -dp.DatePublished.NsSinceEpoch
		} else if err := marshal.Unmarshal(cv, &du); err == nil {
			d = -du.DateUpdated.NsSinceEpoch
		}

		// Index by date
		byDate.SetInsert([]types.Value{types.Number(d)}, cv)

		// Index by tag, then date
		moreTags := map[types.String]int{}
		var wt struct{ Tags []string }
		if err = marshal.Unmarshal(cv, &wt); err == nil {
			for _, t := range wt.Tags {
				byTag.SetInsert([]types.Value{types.String(t), types.Number(d)}, cv)
				moreTags[types.String(t)]++
			}
		}

		// Index by face, then date
		moreFaces := map[types.String]int{}
		var wf struct {
			Faces []struct {
				Name       string
				X, Y, W, H float32
			}
		}
		if err = marshal.Unmarshal(cv, &wf); err == nil {
			for _, f := range wf.Faces {
				byFace.SetInsert([]types.Value{types.String(f.Name), types.Number(d)}, cv)
				moreFaces[types.String(f.Name)]++
			}
		}

		// Index by source, then date
		moreSources := map[types.String]int{}
		var ws struct {
			Sources []string
		}
		if err = marshal.Unmarshal(cv, &ws); err == nil {
			for _, s := range ws.Sources {
				bySource.SetInsert([]types.Value{types.String(s), types.Number(d)}, cv)
			}
		}

		countsMtx.Lock()
		for tag, count := range moreTags {
			tagCounts[tag] += count
		}
		for face, count := range moreFaces {
			faceCounts[face] += count
		}
		for source, count := range moreSources {
			sourceCounts[source] += count
		}
		countsMtx.Unlock()
	}

	groups := []types.Value{}
	inGroups := map[hash.Hash]struct{}{}
	if *groupsStr != "" {
		groups, err = spec.ReadAbsolutePaths(db, *groupsStr)
		d.CheckErrorNoUsage(err)
		walk.WalkValues(groups[0], db, func(cv types.Value) (stop bool) {
			var pg PhotoGroup
			if err := marshal.Unmarshal(cv, &pg); err == nil {
				stop = true
				// TODO: Don't need to do this second arg separately when decoder can catch full value.
				addToIndex(pg.Cover, cv.(types.Struct).Get("cover"))
				inGroups[cv.(types.Struct).Get("cover").Hash()] = struct{}{}
				pg.Photos.IterAll(func(cv types.Value) {
					inGroups[cv.Hash()] = struct{}{}
				})
			}
			return
		})
	}

	for _, v := range inputs {
		walk.WalkValues(v, db, func(cv types.Value) (stop bool) {
			var p Photo
			if _, ok := inGroups[cv.Hash()]; ok {
				stop = true
			} else if err := marshal.Unmarshal(cv, &p); err == nil {
				stop = true
				addToIndex(p, cv)
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
