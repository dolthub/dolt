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

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var stat bool

var nomsDiff = &util.Command{
	Run:       runDiff,
	UsageLine: "diff [--stat] <object1> <object2>",
	Short:     "Shows the difference between two objects",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object arguments.",
	Flags:     setupDiffFlags,
	Nargs:     2,
}

func setupDiffFlags() *flag.FlagSet {
	diffFlagSet := flag.NewFlagSet("diff", flag.ExitOnError)
	diffFlagSet.BoolVar(&stat, "stat", false, "Writes a summary of the changes instead")
	outputpager.RegisterOutputpagerFlags(diffFlagSet)
	verbose.RegisterVerboseFlags(diffFlagSet)

	return diffFlagSet
}

func runDiff(ctx context.Context, args []string) int {
	cfg := config.NewResolver()
	db1, vrw1, value1, err := cfg.GetPath(ctx, args[0])
	util.CheckErrorNoUsage(err)
	if value1 == nil {
		util.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[0]))
	}
	defer db1.Close()

	db2, vrw2, value2, err := cfg.GetPath(ctx, args[1])
	util.CheckErrorNoUsage(err)
	if value2 == nil {
		util.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[1]))
	}
	defer db2.Close()

	d.PanicIfFalse(vrw1.Format() == vrw2.Format())

	if stat {
		diff.Summary(ctx, vrw1, vrw2, value1, value2)
		return 0
	}

	pgr := outputpager.Start()
	defer pgr.Stop()

	diff.PrintDiff(ctx, pgr.Writer, value1, value2, false)
	return 0
}
