// Copyright 2021 Dolthub, Inc.
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
	"flag"
	"fmt"
	"math/rand"

	"github.com/dolthub/dolt/go/store/metrics"
	"github.com/dolthub/dolt/go/store/types"
)

var Dir = flag.String("dir", ".", "directory of the repository")
var Branch = flag.String("branch", "master", "branch of the repository")
var Table = flag.String("table", "", "table to test against")
var Seed = flag.Int("seed", 1, "seed to use for rng key selector")

var NewChunker = flag.Bool("new", false, "use new smooth chunker")

func main() {
	flag.Parse()

	ctx := context.Background()

	maps, err := CollectMaps(ctx, *Dir, *Branch, *Table)
	if err != nil {
		panic(err)
	}
	printSummary(maps)

	if *NewChunker {
		types.SmoothChunking = true
	}

	err = TestWriteAmplification(ctx, int64(*Seed), maps)
	if err != nil {
		panic(err)
	}

	fmt.Println("disco")
}

func printSummary(maps map[string]types.Map) {
	fmt.Println("Test Tables")
	for name, m := range maps {
		fmt.Printf("%s: %8d rows\n", name, m.Len())
	}
}

type WriteAmpExperiment interface {
	Name(table string) string
	Setup(ctx context.Context, m types.Map, seed int64, samples int64) error
	Run(ctx context.Context) (*WriteAmplificationStats, error)
	TearDown(ctx context.Context) error
}

func TestWriteAmplification(ctx context.Context, seed int64, maps map[string]types.Map) (err error) {
	experiments := []WriteAmpExperiment{
		//&DeleteExperiment{editSize: 1},
		//&DeleteExperiment{editSize: 2},
		//&DeleteExperiment{editSize: 10},
		//&DeleteExperiment{editSize: 50},
		&InsertExperiment{editSize: 1},
		&InsertExperiment{editSize: 2},
		&InsertExperiment{editSize: 5},
		&InsertExperiment{editSize: 10},
		&InsertExperiment{editSize: 200},
	}

	const samples = 50

	for name, rows := range maps {
		for _, exp := range experiments {
			suf := fmt.Sprintf(" %d times", samples)
			fmt.Println(exp.Name(name) + suf)

			err = exp.Setup(ctx, rows, seed, samples)
			if err != nil {
				return err
			}

			results, err := exp.Run(ctx)
			if err != nil {
				return err
			}

			err = exp.TearDown(ctx)
			if err != nil {
				return err
			}

			fmt.Println(results.Summary(samples))
		}
	}

	return
}

type InsertExperiment struct {
	orig, mod types.Map

	editSize uint64
	edits    []types.KVPSlice

	stats *WriteAmplificationStats
}

var _ WriteAmpExperiment = &InsertExperiment{}

func (ie *InsertExperiment) Name(table string) string {
	return fmt.Sprintf("Insert %d rows into %s", ie.editSize, table)
}

func (ie *InsertExperiment) Setup(ctx context.Context, m types.Map, seed int64, samples int64) (err error) {
	ie.orig = m
	ie.stats = &WriteAmplificationStats{
		stats: make([]metrics.Histogram, m.Height()),
	}
	ie.edits = make([]types.KVPSlice, samples)

	src := rand.NewSource(seed)
	for i := range ie.edits {
		end := (ie.orig.Len() - ie.editSize)
		pos := uint64(src.Int63()) % end

		iter, err := ie.orig.IteratorAt(ctx, pos)
		if err != nil {
			return err
		}

		ie.edits[i] = make(types.KVPSlice, ie.editSize)
		for j := range ie.edits[i] {
			k, v, err := iter.Next(ctx)
			if err != nil {

			}
			ie.edits[i][j] = types.KVP{Key: k, Val: v}
		}
	}

	edit := ie.orig.Edit()
	for _, sl := range ie.edits {
		for _, kvp := range sl {
			edit.Remove(kvp.Key)
		}
	}
	ie.mod, err = edit.Map(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (ie *InsertExperiment) collect(stats []types.WriteStats) {
	ie.stats.Sample(stats)
}

func (ie *InsertExperiment) Run(ctx context.Context) (*WriteAmplificationStats, error) {
	types.ChunkWithStats = true
	types.WriteStatSink = ie.collect

	for _, sl := range ie.edits {
		edit := ie.mod.Edit()
		for _, kvp := range sl {
			edit.Set(kvp.Key, kvp.Val)
		}
		_, err := edit.Map(ctx)
		if err != nil {
			return nil, err
		}
	}

	return ie.stats, nil
}

func (ie *InsertExperiment) TearDown(ctx context.Context) error {
	types.ChunkWithStats = false
	return nil
}

type DeleteExperiment struct {
	orig types.Map

	editSize uint64
	edits    []uint64

	stats *WriteAmplificationStats
}

var _ WriteAmpExperiment = &DeleteExperiment{}

func (de *DeleteExperiment) Name(table string) string {
	return fmt.Sprintf("Delete %d rows from %s", de.editSize, table)
}

func (de *DeleteExperiment) Setup(ctx context.Context, m types.Map, seed int64, samples int64) error {
	types.ChunkWithStats = true
	types.WriteStatSink = de.collect

	de.orig = m
	de.stats = &WriteAmplificationStats{
		stats: make([]metrics.Histogram, m.Height()),
	}

	src := rand.NewSource(seed)
	de.edits = make([]uint64, samples)
	for i := range de.edits {
		limit := de.orig.Len() - de.editSize
		de.edits[i] = uint64(src.Int63()) % limit
	}

	return nil
}

func (de *DeleteExperiment) collect(stats []types.WriteStats) {
	de.stats.Sample(stats)
}

func (de *DeleteExperiment) Run(ctx context.Context) (*WriteAmplificationStats, error) {
	for _, pos := range de.edits {
		iter, err := de.orig.IteratorAt(ctx, pos)
		if err != nil {
			return nil, err
		}

		edit := de.orig.Edit()
		for i := uint64(0); i < de.editSize; i++ {
			k, _, err := iter.Next(ctx)
			if err != nil {
				return nil, err
			}
			edit.Remove(k)
		}

		_, err = edit.Map(ctx)
		if err != nil {
			return nil, err
		}
	}

	return de.stats, nil
}

func (de *DeleteExperiment) TearDown(ctx context.Context) error {
	types.ChunkWithStats = false
	return nil
}
