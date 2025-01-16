// Copyright 2019 Dolthub, Inc.
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
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/profile"
)

var (
	dir    = flag.String("dir", "", "Write to an NBS store in the given directory")
	table  = flag.String("table", "", "Write to an NBS store in AWS, using this table")
	bucket = flag.String("bucket", "", "Write to an NBS store in AWS, using this bucket")
	dbName = flag.String("db", "", "Write to an NBS store in AWS, using this db name")
)

const memTableSize = 128 * humanize.MiByte

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	ctx := context.Background()

	profile.RegisterProfileFlags(flag.CommandLine)
	flag.Parse(true)

	if flag.NArg() != 0 {
		flag.Usage()
		return
	}

	var store *nbs.NomsBlockStore
	if *dir != "" {
		var err error
		store, err = nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), *dir, memTableSize, nbs.NewUnlimitedMemQuotaProvider())
		d.PanicIfError(err)

		*dbName = *dir
	} else if *table != "" && *bucket != "" && *dbName != "" {
		sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))

		var err error
		store, err = nbs.NewAWSStore(context.Background(), types.Format_Default.VersionString(), *table, *dbName, *bucket, s3.New(sess), dynamodb.New(sess), memTableSize, nbs.NewUnlimitedMemQuotaProvider())
		d.PanicIfError(err)
	} else {
		log.Fatalf("Must set either --dir or ALL of --table, --bucket and --db\n")
	}

	vrw := types.NewValueStore(store)

	root, err := store.Root(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to get root: %v\n", err)
		os.Exit(1)
	}

	defer profile.MaybeStartProfile().Stop()

	rootValue, err := vrw.ReadValue(ctx, root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to get root value: %v\n", err)
		os.Exit(1)
	}

	ref, err := types.NewRef(rootValue, types.Format_Default)
	d.PanicIfError(err)
	height := ref.Height()
	fmt.Println("Store is of height", height)
	fmt.Println("| Height |   Nodes | Children | Branching | Groups | Reads | Pruned |")
	fmt.Println("+--------+---------+----------+-----------+--------+-------+--------+")
	chartFmt := "| %6d | %7d | %8d | %9d | %6d | %5d | %6d |\n"

	var optimal, sum int
	visited := make(map[hash.Hash]struct{})

	current := hash.HashSlice{root}
	for numNodes := 1; numNodes > 0; numNodes = len(current) {
		// Start by reading the values of the current level of the graph
		currentValues := make(map[hash.Hash]types.Value, len(current))
		readValues, err := vrw.ReadManyValues(ctx, current)
		d.PanicIfError(err)
		for i, v := range readValues {
			h := current[i]
			currentValues[h] = v
			visited[h] = struct{}{}
		}

		// Iterate all the Values at the current level of the graph IN ORDER (as specified in |current|) and gather up their embedded refs. We'll build two different lists of hash.Hashes during this process:
		// 1) An ordered list of ALL the children of the current level.
		// 2) An ordered list of the child nodes that contain refs to chunks we haven't yet visited. This *excludes* already-visted nodes and nodes without children.
		// We'll use 1) to get an estimate of how good the locality is among the children of the current level, and then 2) to descend to the next level of the graph.
		orderedChildren := hash.HashSlice{}
		nextLevel := hash.HashSlice{}
		for _, h := range current {
			_ = types.WalkAddrs(currentValues[h], types.Format_Default, func(h hash.Hash, isleaf bool) error {
				orderedChildren = append(orderedChildren, h)
				if _, ok := visited[h]; !ok && !isleaf {
					nextLevel = append(nextLevel, h)
				}
				return nil
			})
		}

		// Estimate locality among the members of |orderedChildren| by splitting into groups that are roughly |branchFactor| in size and calling CalcReads on each group. With perfect locality, we'd expect that each group could be read in a single physical read.
		numChildren := len(orderedChildren)
		branchFactor := numChildren / numNodes
		numGroups := numNodes
		if numChildren%numNodes != 0 {
			numGroups++
		}
		wg := &sync.WaitGroup{}
		reads := make([]int, numGroups)
		for i := 0; i < numGroups; i++ {
			wg.Add(1)
			if i+1 == numGroups { // last group
				go func(i int) {
					defer wg.Done()
					reads[i], _, _, err = nbs.CalcReads(store, orderedChildren[i*branchFactor:].HashSet(), 0, nil)
					d.PanicIfError(err)
				}(i)
				continue
			}
			go func(i int) {
				defer wg.Done()
				reads[i], _, _, err = nbs.CalcReads(store, orderedChildren[i*branchFactor:(i+1)*branchFactor].HashSet(), 0, nil)
				d.PanicIfError(err)
			}(i)
		}

		wg.Wait()

		sumOfReads := sumInts(reads)
		fmt.Printf(chartFmt, height, numNodes, numChildren, branchFactor, numGroups, sumOfReads, numChildren-len(nextLevel))

		sum += sumOfReads
		optimal += numGroups
		height--
		current = nextLevel
	}

	fmt.Printf("\nVisited %d chunk groups\n", optimal)
	fmt.Printf("Reading DB %s requires %.01fx optimal number of reads\n", *dbName, float64(sum)/float64(optimal))
}

func sumInts(nums []int) (sum int) {
	for _, n := range nums {
		sum += n
	}
	return
}
