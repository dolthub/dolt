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
	"github.com/attic-labs/noms/go/util/random"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/go/walk"
	flag "github.com/juju/gnuflag"
)

type Date struct {
	NsSinceEpoch float64
}

type Photo struct {
	Id        string
	DateTaken Date
}

type PhotoGroup struct {
	Id     string
	Cover  types.Value
	Photos types.Set
}

func main() {
	if !index() {
		exit.Fail()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "photo-dedup-by-date dedups photos by date taken time\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] [input-paths...]\n\n", path.Base(os.Args[0]))
	fmt.Fprintln(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

func index() (win bool) {
	var dbStr = flag.String("db", "", "input database spec")
	var outDSStr = flag.String("out-ds", "", "output dataset to write to - if empty, defaults to input dataset")
	var thresh = flag.Int("threshold", 5000, "Number of milliseconds within which to consider photos duplicates")
	verbose.RegisterVerboseFlags(flag.CommandLine)

	flag.Usage = usage
	flag.Parse(true)

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
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return
	}

	byDate := buildDateIndex(db, inputs)
	groups := buildGroups(db, *thresh, byDate)

	val, err := marshal.Marshal(struct {
		ByDate types.Map
		Groups types.List
	}{
		byDate,
		groups,
	})
	d.Chk.NoError(err)

	meta, err := marshal.Marshal(struct {
		Date string
	}{
		time.Now().Format(spec.CommitMetaDateFormat),
	})
	d.Chk.NoError(err)

	outDS, err = db.Commit(outDS, val, datas.CommitOptions{
		Meta: meta.(types.Struct),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not commit: %s\n", err)
		return
	}
	return true
}

func buildDateIndex(db types.ValueReadWriter, inputs []types.Value) types.Map {
	indexBuilder := types.NewGraphBuilder(db, types.MapKind, true)
	for _, v := range inputs {
		walk.WalkValues(v, db, func(cv types.Value) (stop bool) {
			var p Photo
			if err := marshal.Unmarshal(cv, &p); err == nil {
				stop = true
				if p.DateTaken.NsSinceEpoch != 0 {
					indexBuilder.SetInsert(
						[]types.Value{types.Number(float64(p.DateTaken.NsSinceEpoch))},
						cv)
				}
			}
			return
		})
	}
	return indexBuilder.Build().(types.Map)
}

func buildGroups(db types.ValueReadWriter, thresh int, byDate types.Map) types.List {
	vals := make(chan types.Value)
	groupBuilder := types.NewStreamingList(db, vals)

	var group *PhotoGroup

	startGroup := func(first types.Value) {
		group = &PhotoGroup{
			Id:     random.Id(),
			Cover:  first,
			Photos: types.NewSet(),
		}
	}

	flush := func() {
		if group != nil && group.Photos.Len() > 0 {
			v, err := marshal.Marshal(*group)
			d.Chk.NoError(err)
			vals <- v
			group = nil
		}
	}

	lastTime := -math.MaxFloat64
	byDate.IterAll(func(key, s types.Value) {
		s.(types.Set).IterAll(func(val types.Value) {
			dt := float64(key.(types.Number))
			if (dt - lastTime) > float64(thresh*1e6) {
				flush()
				startGroup(val)
			} else {
				group.Photos = group.Photos.Insert(val)
			}
			lastTime = dt
		})
		return
	})
	flush()
	close(vals)

	return <-groupBuilder
}
