// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/attic-labs/noms/go/walk"
	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

var (
	inPathArg    = ""
	outDsArg     = ""
	relPathArg   = ""
	txRegexArg   = ""
	txReplaceArg = ""
	txConvertArg = ""
)

var longUpHelp = `'nomdex up' builds indexes that are useful for rapidly accessing objects.

This sample tool can index objects based on any string or number attribute of that
object. The 'up' command works by scanning all the objects reachable from the --in-path
command line argument. It tests the object to determine if there is a string or number
value reachable by applying the --by path argument to the object. If so, the object is
added to the index under that value. 

For example, if there are objects in the database that contain a personId and a
gender field, 'nomdex up' can scan all the objects in a given dataset and build
an index on the specified field with the following commands:
   nomdex up --in-path <dsSpec>.value --by .gender --out-ds gender-index
   nomdex up --in-path <dsSpec>.value --by .address.city --out-ds personId-index

The previous commands can be understood as follows. The first command updates or
builds an index by scanning all the objects that are reachable from |in-path| that
have a string or number value reachable using |by| and stores the root of the
resulting index in a dataset specified by |out-ds|.

Notice that the --in-path argument has a value of '<dsSpec>.value'. The '.value'
is not strictly necessary but it's normally useful when indexing. Since datasets
generally point to Commit objects in Noms, they usually have parents which are
previous versions of the data. If you add .value to the end of the dataset, only
the most recent version of the data will be indexed. Without the '.value' all
objects in all previous commits will also be indexed which is most often not what
is expected.

There are three additional commands that can be useful for transforming the value
being indexed:
    * tx-replace: used to modify behavior of tx-regex, see below
    * tx-regex: the behavior for this argument depends on whether a tx-replace argument
        is present. If so, the go routine "regexp.ReplaceAllString() is called:
            txRe := regex.MustCompile(|tx-regex|)
            txRe.ReplaceAllString(|index value|, |tx-replace|
        If tx-replace is not present then the following call is made on each value:
            txRe := regex.MustCompile(|tx-regex|)
            regex.FindStringSubmatch(|index value|)
    *tx-convert: attempts to convert the index value to the type specified.
        Currently the only value accepted for this arg is 'number'
        
The resulting indexes can be used by the 'nomdex find command' for help on that
see: nomdex find -h
`

var update = &util.Command{
	Run:       runUpdate,
	UsageLine: "up --in-path <path> --out-ds <dspath> --by <relativepath>",
	Short:     "Build/Update an index",
	Long:      longUpHelp,
	Flags:     setupUpdateFlags,
	Nargs:     0,
}

func setupUpdateFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("up", flag.ExitOnError)
	flagSet.StringVar(&inPathArg, "in-path", "", "a value to search for items to index within ")
	flagSet.StringVar(&outDsArg, "out-ds", "", "name of dataset to save the results to")
	flagSet.StringVar(&relPathArg, "by", "", "a path relative to all the items in <in-path> to index by")
	flagSet.StringVar(&txRegexArg, "tx-regex", "", "perform a string transformation on value before putting it in index")
	flagSet.StringVar(&txReplaceArg, "tx-replace", "", "replace values matched by tx-regex")
	flagSet.StringVar(&txConvertArg, "tx-convert", "", "convert the result of a tx regex/replace to this type (only does 'number' currently)")
	verbose.RegisterVerboseFlags(flagSet)
	profile.RegisterProfileFlags(flagSet)
	return flagSet
}

type StreamingSetEntry struct {
	valChan chan<- types.Value
	setChan <-chan types.Set
}

type IndexMap map[types.Value]StreamingSetEntry

type Index struct {
	m          IndexMap
	indexedCnt int64
	seenCnt    int64
	mutex      sync.Mutex
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

	defer profile.MaybeStartProfile().Stop()

	cfg := config.NewResolver()
	db, rootObject, err := cfg.GetPath(inPathArg)
	d.Chk.NoError(err)

	if rootObject == nil {
		fmt.Printf("Object not found: %s\n", inPathArg)
		return 1
	}

	outDs := db.GetDataset(outDsArg)
	relPath, err := types.ParsePath(relPathArg)
	if printError(err, "Error parsing -by value\n\t") {
		return 1
	}

	gb := types.NewGraphBuilder(db, types.MapKind, true)
	addElementsToGraphBuilder(gb, db, rootObject, relPath)
	indexMap := gb.Build().(types.Map)

	outDs, err = db.Commit(outDs, indexMap, datas.CommitOptions{})
	d.Chk.NoError(err)
	fmt.Printf("Committed index with %d entries to dataset: %s\n", indexMap.Len(), outDsArg)

	return 0
}

func addElementsToGraphBuilder(gb *types.GraphBuilder, db datas.Database, rootObject types.Value, relPath types.Path) {
	typeCacheMutex := sync.Mutex{}
	typeCache := map[*types.Type]bool{}

	var txRe *regexp.Regexp
	if txRegexArg != "" {
		var err error
		txRe, err = regexp.Compile(txRegexArg)
		d.CheckError(err)
	}

	index := Index{m: IndexMap{}}
	walk.WalkValues(rootObject, db, func(v types.Value) bool {
		typ := v.Type()
		typeCacheMutex.Lock()
		hasPath, ok := typeCache[typ]
		typeCacheMutex.Unlock()
		if !ok || hasPath {
			pathResolved := false
			tv := relPath.Resolve(v)
			if tv != nil {
				index.addToGraphBuilder(gb, tv, v, txRe)
				pathResolved = true
			}
			if !ok {
				typeCacheMutex.Lock()
				typeCache[typ] = pathResolved
				typeCacheMutex.Unlock()
			}
		}
		return false
	})

	status.Done()
}

func (idx *Index) addToGraphBuilder(gb *types.GraphBuilder, k, v types.Value, txRe *regexp.Regexp) {
	atomic.AddInt64(&idx.seenCnt, 1)
	if txRe != nil {
		k1 := types.EncodedValue(k)
		k2 := ""
		if txReplaceArg != "" {
			k2 = txRe.ReplaceAllString(string(k1), txReplaceArg)
		} else {
			matches := txRe.FindStringSubmatch(string(k1))
			if len(matches) > 0 {
				k2 = matches[len(matches)-1]
			}
		}
		if txConvertArg == "number" {
			if k2 == "" {
				return
			}
			n, err := strconv.ParseFloat(k2, 64)
			if err != nil {
				fmt.Println("error converting to number: ", err)
				return
			}
			k = types.Number(n)
		} else {
			k = types.String(k2)
		}
	}
	atomic.AddInt64(&idx.indexedCnt, 1)
	gb.SetInsert(types.ValueSlice{k}, v)
	status.Printf("Found %s objects, Indexed %s objects", humanize.Comma(idx.seenCnt), humanize.Comma(idx.indexedCnt))
}
