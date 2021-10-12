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

	"github.com/dolthub/dolt/go/store/types"
)

var Dir = flag.String("dir", ".", "directory of the repository")
var Branch = flag.String("branch", "master", "branch of the repository")
var Table = flag.String("table", "", "table to test against")
var Seed = flag.Int("seed", 1, "seed to use for rng key selector")
var Verbose = flag.Bool("verbose", false, "detail stats by level")

func main() {
	flag.Parse()

	ctx := context.Background()

	maps, err := CollectMaps(ctx, *Dir, *Branch, *Table)
	if err != nil {
		panic(err)
	}

	err = TestWriteAmplification(ctx, int64(*Seed), maps)
	if err != nil {
		panic(err)
	}

	fmt.Println("disco")
}

const samples = 100

func RunExperiment(v Variables, fn func() error) (err error) {
	// save previous values
	prevSmooth := types.SmoothChunking

	// set test values
	types.SmoothChunking = v.smooth

	err = fn()

	// restore previous values
	types.SmoothChunking = prevSmooth

	return err
}

type Variables struct {
	smooth bool
}

func (v Variables) String() string {
	return fmt.Sprintf("Smooth Chunking %t", v.smooth)
}

type WriteAmpTest interface {
	Name(table string) string
	Setup(ctx context.Context, m types.Map, seed int64) error
	Run(ctx context.Context) (*WriteAmplificationStats, error)
	TearDown(ctx context.Context) error
}

func TestWriteAmplification(ctx context.Context, seed int64, maps map[string]types.Map) (err error) {
	tests := []WriteAmpTest{
		&DeleteTest{editSize: 1},
		&DeleteTest{editSize: 10},
		&DeleteTest{editSize: 50},
		&InsertTest{editSize: 1},
		&InsertTest{editSize: 10},
		&InsertTest{editSize: 50},
	}

	experiments := []Variables{
		{smooth: false},
		{smooth: true},
	}

	for name, rows := range maps {

		for _, test := range tests {
			fmt.Println(fmt.Sprintf("---------- %s ----------", test.Name(name)))

			for _, vars := range experiments {
				fmt.Println(vars.String())

				err = test.Setup(ctx, rows, seed)
				if err != nil {
					return err
				}

				var results *WriteAmplificationStats
				err = RunExperiment(vars, func() error {
					results, err = test.Run(ctx)
					return err
				})
				if err != nil {
					return err
				}

				err = test.TearDown(ctx)
				if err != nil {
					return err
				}

				if *Verbose {
					fmt.Println(results.SummaryByLevel())
				} else {
					fmt.Println(results.Summary())
				}
			}
		}
	}

	return
}

type InsertTest struct {
	orig, mod types.Map

	editSize uint64
	edits    []types.KVPSlice

	stats *WriteAmplificationStats
}

var _ WriteAmpTest = &InsertTest{}

func (ie *InsertTest) Name(table string) string {
	return fmt.Sprintf("Insert %d rows into %s %d times", ie.editSize, table, samples)
}

func (ie *InsertTest) Setup(ctx context.Context, m types.Map, seed int64) (err error) {
	ie.orig = m
	ie.stats = NewWriteAmplificationStats(m.Height())
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

func (ie *InsertTest) collect(stats []types.WriteStats) {
	ie.stats.Sample(stats)
}

func (ie *InsertTest) Run(ctx context.Context) (*WriteAmplificationStats, error) {
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

func (ie *InsertTest) TearDown(ctx context.Context) error {
	types.ChunkWithStats = false
	return nil
}

type DeleteTest struct {
	orig types.Map

	editSize uint64
	edits    []uint64

	stats *WriteAmplificationStats
}

var _ WriteAmpTest = &DeleteTest{}

func (de *DeleteTest) Name(table string) string {
	return fmt.Sprintf("Delete %d rows from %s %d times", de.editSize, table, samples)
}

func (de *DeleteTest) Setup(ctx context.Context, m types.Map, seed int64) (err error) {
	types.ChunkWithStats = true
	types.WriteStatSink = de.collect

	de.orig = m
	de.stats = NewWriteAmplificationStats(m.Height())

	src := rand.NewSource(seed)
	de.edits = make([]uint64, samples)
	for i := range de.edits {
		limit := de.orig.Len() - de.editSize
		de.edits[i] = uint64(src.Int63()) % limit
	}

	return nil
}

func (de *DeleteTest) collect(stats []types.WriteStats) {
	de.stats.Sample(stats)
}

func (de *DeleteTest) Run(ctx context.Context) (*WriteAmplificationStats, error) {
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

func (de *DeleteTest) TearDown(ctx context.Context) error {
	types.ChunkWithStats = false
	return nil
}
