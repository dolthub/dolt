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
	"github.com/dolthub/dolt/go/store/types"
)

var Dir = flag.String("dir", ".", "directory of the repository")
var Branch = flag.String("branch", "master", "branch of the repository")
var Table = flag.String("table", "", "table to test against")
var Seed = flag.Int("seed", 1, "seed to use for rng key selector")

func main() {
	flag.Parse()

	ctx := context.Background()

	maps, err := CollectMaps(ctx, *Dir, *Branch, *Table)
	if err != nil {
		panic(err)
	}
	printSummary(maps)

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
	Setup(ctx context.Context, m types.Map, seed int64) error
	Test(ctx context.Context) ([]interface{}, error)
}

func TestWriteAmplification(ctx context.Context, seed int64, maps map[string]types.Map) (err error) {
	tests := []WriteAmpExperiment{
		DeleteExperiment{editSize: 1, numEdits: 50},
		DeleteExperiment{editSize: 2, numEdits: 50},
		DeleteExperiment{editSize: 5, numEdits: 50},
		DeleteExperiment{editSize: 10, numEdits: 50},
		InsertExperiment{editSize: 1, numEdits: 50},
		InsertExperiment{editSize: 2, numEdits: 50},
		InsertExperiment{editSize: 5, numEdits: 50},
		InsertExperiment{editSize: 10, numEdits: 50},
	}

	for _, test := range tests {
		for _, table := range maps {
			err = test.Setup(ctx, table, seed)
			if err != nil {
				return err
			}

			results, err := test.Test(ctx)
			if err != nil {
				return err
			}

			fmt.Println(results)
		}
	}

	return
}

type InsertExperiment struct {
	orig, mod types.Map
	edits     []types.KVPSlice

	numEdits uint32
	editSize uint32
}

var _ WriteAmpExperiment = InsertExperiment{}

func (ie InsertExperiment) Name(table string) string {
	return fmt.Sprintf("Insert %d rows into %s", ie.editSize, table)
}

func (ie InsertExperiment) Setup(ctx context.Context, m types.Map, seed int64) error {
	ie.orig = m
	return nil
}

func (ie InsertExperiment) Test(context.Context) ([]interface{}, error) {
	return nil, nil
}

type DeleteExperiment struct {
	orig  types.Map
	edits []types.ValueSlice

	numEdits uint32
	editSize uint32
}

var _ WriteAmpExperiment = DeleteExperiment{}

func (de DeleteExperiment) Name(table string) string {
	return fmt.Sprintf("Delete %d rows from %s", de.editSize, table)
}

func (de DeleteExperiment) Setup(ctx context.Context, m types.Map, seed int64) error {
	de.orig = m
	return nil
}

func (de DeleteExperiment) Test(context.Context) ([]interface{}, error) {
	return nil, nil
}
