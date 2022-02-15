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

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var toDelete string

var nomsDs = &util.Command{
	Run:       runDs,
	UsageLine: "ds [<database> | -d <dataset>]",
	Short:     "Noms dataset management",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database and dataset arguments.",
	Flags:     setupDsFlags,
	Nargs:     0,
}

func setupDsFlags() *flag.FlagSet {
	dsFlagSet := flag.NewFlagSet("ds", flag.ExitOnError)
	dsFlagSet.StringVar(&toDelete, "d", "", "dataset to delete")
	verbose.RegisterVerboseFlags(dsFlagSet)
	return dsFlagSet
}

func runDs(ctx context.Context, args []string) int {
	cfg := config.NewResolver()
	if toDelete != "" {
		db, _, set, err := cfg.GetDataset(ctx, toDelete)
		util.CheckError(err)
		defer db.Close()

		oldCommitRef, errBool, err := set.MaybeHeadRef()
		d.PanicIfError(err)

		if !errBool {
			util.CheckError(fmt.Errorf("Dataset %v not found", set.ID()))
		}

		_, err = set.Database().Delete(ctx, set)
		util.CheckError(err)

		fmt.Printf("Deleted %v (was #%v)\n", toDelete, oldCommitRef.TargetHash().String())
	} else {
		dbSpec := ""
		if len(args) >= 1 {
			dbSpec = args[0]
		}
		store, _, err := cfg.GetDatabase(ctx, dbSpec)
		util.CheckError(err)
		defer store.Close()

		dss, err := store.Datasets(ctx)

		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to get datasets")
			return 1
		}

		_ = dss.IterAll(ctx, func(k string, _ hash.Hash) error {
			fmt.Println(k)
			return nil
		})
	}
	return 0
}
