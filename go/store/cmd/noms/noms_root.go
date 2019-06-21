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
	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/config"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
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
	d.CheckErrorNoUsage(err)

	currRoot := cs.Root(ctx)

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
	db, err := cfg.GetDatabase(ctx, args[0])
	d.CheckErrorNoUsage(err)
	defer db.Close()
	if !validate(ctx, db.ReadValue(ctx, h)) {
		return 1
	}

	fmt.Println(`WARNING

This operation replaces the entire database with the instance having the given
hash. The old database becomes eligible for GC.

ANYTHING NOT SAVED WILL BE LOST

Continue?`)

	var input string
	n, err := fmt.Scanln(&input)
	d.CheckErrorNoUsage(err)
	if n != 1 || strings.ToLower(input) != "y" {
		return 0
	}

	ok = cs.Commit(ctx, h, currRoot)
	if !ok {
		fmt.Fprintln(os.Stderr, "Optimistic concurrency failure")
		return 1
	}

	fmt.Printf("Success. Previous root was: %s\n", currRoot)
	return 0
}

func validate(ctx context.Context, r types.Value) bool {
	rootType := types.MakeMapType(types.StringType, types.MakeRefType(types.ValueType))
	if !types.IsValueSubtypeOf(r, rootType) {
		fmt.Fprintf(os.Stderr, "Root of database must be %s, but you specified: %s\n", rootType.Describe(ctx), types.TypeOf(r).Describe(ctx))
		return false
	}

	return r.(types.Map).Any(ctx, func(k, v types.Value) bool {
		if !datas.IsRefOfCommitType(types.TypeOf(v)) {
			fmt.Fprintf(os.Stderr, "Invalid root map. Value for key '%s' is not a ref of commit.", string(k.(types.String)))
			return false
		}
		return true
	})
}
