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
	"os"
	"strings"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var nomsRoot = &util.Command{
	Run:       runRoot,
	UsageLine: "root <db-spec>",
	Short:     "Get or set the current root hash of the entire database",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.",
	Flags:     setupRootFlags,
	Nargs:     1,
}

var updateRoot = ""

func setupRootFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("root", flag.ExitOnError)
	flagSet.StringVar(&updateRoot, "update", "", "Replaces the entire database with the one with the given hash")
	return flagSet
}

func runRoot(ctx context.Context, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Not enough arguments")
		return 0
	}

	cfg := config.NewResolver()
	cs, err := cfg.GetChunkStore(ctx, args[0])
	util.CheckErrorNoUsage(err)

	currRoot, err := cs.Root(ctx)

	if err != nil {
		fmt.Fprintln(os.Stderr, "error getting root.", err)
		return 1
	}

	if updateRoot == "" {
		fmt.Println(currRoot)
		return 0
	}

	if updateRoot[0] == '#' {
		updateRoot = updateRoot[1:]
	}
	h, ok := hash.MaybeParse(updateRoot)
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid hash: %s\n", h.String())
		return 1
	}

	// If BUG 3407 is correct, we might be able to just take cs and make a Database directly from that.
	db, vrw, err := cfg.GetDatabase(ctx, args[0])
	util.CheckErrorNoUsage(err)
	defer db.Close()
	v, err := vrw.ReadValue(ctx, h)
	util.CheckErrorNoUsage(err)
	if !validate(ctx, vrw.Format(), v) {
		return 1
	}

	fmt.Println(`WARNING

This operation replaces the entire database with the instance having the given
hash. The old database becomes eligible for GC.

ANYTHING NOT SAVED WILL BE LOST

Continue?`)

	var input string
	n, err := fmt.Scanln(&input)
	util.CheckErrorNoUsage(err)
	if n != 1 || strings.ToLower(input) != "y" {
		return 0
	}

	ok, err = cs.Commit(ctx, h, currRoot)

	if err != nil {
		fmt.Fprintf(os.Stderr, "commit error: %s", err.Error())
		return 1
	}

	if !ok {
		fmt.Fprintln(os.Stderr, "Optimistic concurrency failure")
		return 1
	}

	fmt.Printf("Success. Previous root was: %s\n", currRoot)
	return 0
}

func mustType(t *types.Type, err error) *types.Type {
	d.PanicIfError(err)
	return t
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustValue(v types.Value, err error) types.Value {
	d.PanicIfError(err)
	return v
}

func mustGetValue(v types.Value, ok bool, err error) types.Value {
	d.PanicIfError(err)
	d.PanicIfFalse(ok)
	return v
}

func mustSet(s types.Set, err error) types.Set {
	d.PanicIfError(err)
	return s
}

func mustList(l types.List, err error) types.List {
	d.PanicIfError(err)
	return l
}

func validate(ctx context.Context, nbf *types.NomsBinFormat, r types.Value) bool {
	rootType := mustType(types.MakeMapType(types.PrimitiveTypeMap[types.StringKind], mustType(types.MakeRefType(types.PrimitiveTypeMap[types.ValueKind]))))
	if isSub, err := types.IsValueSubtypeOf(nbf, r, rootType); err != nil {
		panic(err)
	} else if !isSub {
		fmt.Fprintf(os.Stderr, "Root of database must be %s, but you specified: %s\n", mustString(rootType.Describe(ctx)), mustString(mustType(types.TypeOf(r)).Describe(ctx)))
		return false
	}

	return true
}
