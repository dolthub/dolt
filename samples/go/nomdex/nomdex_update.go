// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/walk"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

var (
	inPathArg  = ""
	outDsArg   = ""
	relPathArg = ""
)

var update = &util.Command{
	Run:       runUpdate,
	UsageLine: "up --in-path <path> --out-ds <dspath> --by <relativepath>",
	Short:     "Build/Update an index",
	Long:      "Traverse all values starting at root and add values found at 'relativePath' to a map found at 'out-ds'\n",
	Flags:     setupUpdateFlags,
	Nargs:     0,
}

func setupUpdateFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("up", flag.ExitOnError)
	flagSet.StringVar(&inPathArg, "in-path", "", "a value to search for items to index within ")
	flagSet.StringVar(&outDsArg, "out-ds", "", "name of dataset to save the results to")
	flagSet.StringVar(&relPathArg, "by", "", "a path relative to all the items in <in-path> to index by")
	return flagSet
}

type StreamingSetEntry struct {
	valChan chan<- types.Value
	setChan <-chan types.Set
}

type IndexMap map[types.Value]StreamingSetEntry

type Index struct {
	m     IndexMap
	cnt   int64
	mutex sync.Mutex
}

func runUpdate(args []string) int {
	requiredArgs := map[string]string{"in-path": inPathArg, "out-ds": outDsArg, "by": relPathArg}
	for argName, argValue := range requiredArgs {
		if argValue == "" {
			fmt.Fprintf(os.Stderr, "Missing required '%s' arg\n", argName)
			flag.Usage()
			return 1
		}
	}

	db, rootObject, err := spec.GetPath(inPathArg)
	d.Chk.NoError(err)

	if rootObject == nil {
		fmt.Printf("Object not found: %s\n", inPathArg)
		return 1
	}

	outDs := dataset.NewDataset(db, outDsArg)
	relPath, err := types.ParsePath(relPathArg)
	if printError(err, "Error parsing -by value\n\t") {
		return 1
	}

	typeCacheMutex := sync.Mutex{}
	typeCache := map[*types.Type]bool{}

	index := Index{m: IndexMap{}}
	walk.AllP(rootObject, db, func(v types.Value, r *types.Ref) {
		typ := v.Type()
		typeCacheMutex.Lock()
		hasPath, ok := typeCache[typ]
		typeCacheMutex.Unlock()
		if !ok || hasPath {
			pathResolved := false
			tv := relPath.Resolve(v)
			if tv != nil {
				index.Add(db, tv, v)
				pathResolved = true
			}
			if !ok {
				typeCacheMutex.Lock()
				typeCache[typ] = pathResolved
				typeCacheMutex.Unlock()
			}
		}
	}, 4)

	status.Done()
	indexMap := writeToStreamingMap(db, index.m)
	outDs, err = outDs.Commit(indexMap, dataset.CommitOptions{})
	d.Chk.NoError(err)
	fmt.Printf("Committed index with %d entries to dataset: %s\n", indexMap.Len(), outDsArg)
	return 0
}

func (idx *Index) Add(db datas.Database, k, v types.Value) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.cnt++
	se, ok := idx.m[k]
	if !ok {
		valChan := make(chan types.Value)
		setChan := types.NewStreamingSet(db, valChan)
		se = StreamingSetEntry{valChan: valChan, setChan: setChan}
		idx.m[k] = se
	}
	se.valChan <- v
	status.Printf("Indexed %s objects", humanize.Comma(idx.cnt))
}

func writeToStreamingMap(db datas.Database, indexMap IndexMap) types.Map {
	itemCnt := len(indexMap)
	writtenCnt := int64(0)
	indexedCnt := int64(0)
	kvChan := make(chan types.Value)
	mapChan := types.NewStreamingMap(db, kvChan)
	for k, v := range indexMap {
		close(v.valChan)
		s := <-v.setChan
		kvChan <- k
		kvChan <- s
		indexedCnt += int64(s.Len())
		delete(indexMap, k)
		writtenCnt++
		status.Printf("Wrote %s/%d keys, %s indexedObjects", humanize.Comma(writtenCnt), itemCnt, humanize.Comma(indexedCnt))
	}
	close(kvChan)
	status.Done()
	return <-mapChan
}
