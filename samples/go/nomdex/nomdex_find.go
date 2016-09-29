// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var longFindHelp = `'nomdex find' retrieves and prints objects that satisfy the 'query' argument.

Indexes are built using the 'nomdex up' command. For information about building
indexes, see: nomdex up -h

Objects that have been indexed can be quickly found using the nomdex query
language. For example, consider objects with the following type:

struct Person {
  name String,
  geopos struct GeoPos {
     latitude Number,
     longitude Number,
  }
}

Objects of this type can be indexed on the name, latitude and longitude fields
with the following commands:
    nomdex up --in-path ~/nomsdb::people.value --by .name --out-ds by-name
    nomdex up --in-path ~/nomsdb::people.value --by .geopos.latitude --out-ds by-lat
    nomdex up --in-path ~/nomsdb::people.value --by .geopos.longitude --out-ds by-lng
    
The following query could be used to find all people with an address near the
equator:
    nomdex find 'by-lat >= -1.0 and by-lat <= 1.0'

We could also get a list of all people who live near the equator whose name begins with "A":
    nomdex find '(by-name >= "A" and by-name < "B") and (by-lat >= -1.0 and by-lat <= 1.0)'
   
The query language is simple. It currently supports the following relational operators:
    <, <=, >, >=, =, !=
Relational expressions are always of the form:
    <index> <relational operator> <constant>   e.g. personId >= 2000.
    
Indexes are the name given by the --out-ds argument in the 'nomdex up' command.
Constants are either "strings" (in quotes) or numbers (e.g. 3, 3000, -2, -2.5,
3.147, etc).

Relational expressions can be combined using the "and" and "or" operators.
Parentheses can (and should) be used to ensure that the evaluation is done in
the desired order.
`

var find = &util.Command{
	Run:       runFind,
	UsageLine: "find --db <database spec> <query>",
	Short:     "Print objects in index that satisfy 'query'",
	Long:      longFindHelp,
	Flags:     setupFindFlags,
	Nargs:     1,
}

var dbPath = ""

func setupFindFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("find", flag.ExitOnError)
	flagSet.StringVar(&dbPath, "db", "", "database containing index")
	outputpager.RegisterOutputpagerFlags(flagSet)
	verbose.RegisterVerboseFlags(flagSet)
	return flagSet
}

func runFind(args []string) int {
	query := args[0]
	if dbPath == "" {
		fmt.Fprintf(os.Stderr, "Missing required 'index' arg\n")
		flag.Usage()
		return 1
	}

	cfg := config.NewResolver()
	db, err := cfg.GetDatabase(dbPath)
	if printError(err, "Unable to open database\n\terror: ") {
		return 1
	}
	defer db.Close()

	im := &indexManager{db: db, indexes: map[string]types.Map{}}
	expr, err := parseQuery(query, im)
	if err != nil {
		fmt.Printf("err: %s\n", err)
		return 1
	}

	pgr := outputpager.Start()
	defer pgr.Stop()

	iter := expr.iterator(im)
	cnt := 0
	if iter != nil {
		for v := iter.Next(); v != nil; v = iter.Next() {
			types.WriteEncodedValue(pgr.Writer, v)
			fmt.Fprintf(pgr.Writer, "\n")
			cnt++
		}
	}
	fmt.Fprintf(pgr.Writer, "Found %d objects\n", cnt)

	return 0
}

func printObjects(w io.Writer, index types.Map, ranges queryRangeSlice) {
	cnt := 0
	first := true
	printObjectForRange := func(index types.Map, r queryRange) {
		index.IterFrom(r.lower.value, func(k, v types.Value) bool {
			if first && r.lower.value != nil && !r.lower.include && r.lower.value.Equals(k) {
				return false
			}
			if r.upper.value != nil {
				if !r.upper.include && r.upper.value.Equals(k) {
					return true
				}
				if r.upper.value.Less(k) {
					return true
				}
			}
			s := v.(types.Set)
			s.IterAll(func(v types.Value) {
				types.WriteEncodedValue(w, v)
				fmt.Fprintf(w, "\n")
				cnt++
			})
			return false
		})
	}
	for _, r := range ranges {
		printObjectForRange(index, r)
	}
	fmt.Fprintf(w, "Found %d objects\n", cnt)
}

func openIndex(idxName string, im *indexManager) error {
	if _, hasIndex := im.indexes[idxName]; hasIndex {
		return nil
	}

	ds := im.db.GetDataset(idxName)
	commit, ok := ds.MaybeHead()
	if !ok {
		return fmt.Errorf("index '%s' not found", idxName)
	}

	index, ok := commit.Get(datas.ValueField).(types.Map)
	if !ok {
		return fmt.Errorf("Value of commit at '%s' is not a valid index", idxName)
	}

	// Todo: make this type be Map<String | Number>, Set<Value>> once Issue #2326 gets resolved and
	// IsSubtype() returns the correct value.
	typ := types.MakeMapType(
		types.MakeUnionType(types.StringType, types.NumberType),
		types.ValueType)

	if !types.IsSubtype(typ, index.Type()) {
		return fmt.Errorf("%s does not point to a suitable index type:", idxName)
	}

	im.indexes[idxName] = index
	return nil
}
