// Copyright 2019 Liquidata, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/config"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/datetime"
	"github.com/liquidata-inc/ld/dolt/go/store/util/outputpager"
	"github.com/liquidata-inc/ld/dolt/go/store/util/verbose"
)

var nomsShow = &util.Command{
	Run:       runShow,
	UsageLine: "show [flags] <object>",
	Short:     "Shows a serialization of a Noms object",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupShowFlags,
	Nargs:     1,
}

var (
	showRaw   = false
	showStats = false
	showPages = false
	tzName    string
)

func setupShowFlags() *flag.FlagSet {
	showFlagSet := flag.NewFlagSet("show", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(showFlagSet)
	verbose.RegisterVerboseFlags(showFlagSet)
	showFlagSet.BoolVar(&showPages, "page", false, "If true output is shown in an output pager")
	showFlagSet.BoolVar(&showRaw, "raw", false, "If true, dumps the raw binary version of the data")
	showFlagSet.BoolVar(&showStats, "stats", false, "If true, reports statistics related to the value")
	showFlagSet.StringVar(&tzName, "tz", "local", "display formatted date comments in specified timezone, must be: local or utc")
	return showFlagSet
}

func runShow(ctx context.Context, args []string) int {
	cfg := config.NewResolver()
	database, value, err := cfg.GetPath(ctx, args[0])
	util.CheckErrorNoUsage(err)
	defer database.Close()

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", args[0])
		return 0
	}

	if showRaw && showStats {
		fmt.Fprintln(os.Stderr, "--raw and --stats are mutually exclusive")
		return 0
	}

	if showRaw {
		ch := types.EncodeValue(value, database.Format())
		buf := bytes.NewBuffer(ch.Data())
		_, err = io.Copy(os.Stdout, buf)
		util.CheckError(err)
		return 0
	}

	if showStats {
		types.WriteValueStats(ctx, os.Stdout, value, database)
		return 0
	}

	tz, _ := locationFromTimezoneArg(tzName, nil)
	datetime.RegisterHRSCommenter(tz)

	if showPages {
		pgr := outputpager.Start()
		defer pgr.Stop()

		types.WriteEncodedValue(ctx, pgr.Writer, value)
		fmt.Fprintln(pgr.Writer)
	} else {
		types.WriteEncodedValue(ctx, os.Stdout, value)
	}

	return 0
}
