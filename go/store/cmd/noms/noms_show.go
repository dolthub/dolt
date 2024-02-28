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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/datetime"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/dolt/go/store/util/verbose"
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

	var value types.Value
	database, vrw, value, err := cfg.GetPath(ctx, args[0])

	if err != nil {
		util.CheckErrorNoUsage(err)
	} else {
	}

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
		ch, err := types.EncodeValue(value.(types.Value), vrw.Format())
		util.CheckError(err)
		buf := bytes.NewBuffer(ch.Data())
		_, err = io.Copy(os.Stdout, buf)
		util.CheckError(err)
		return 0
	}

	if showStats {
		types.WriteValueStats(ctx, os.Stdout, value.(types.Value), vrw)
		return 0
	}

	tz, _ := locationFromTimezoneArg(tzName, nil)
	datetime.RegisterHRSCommenter(tz)

	if showPages {
		pgr := outputpager.Start()
		defer pgr.Stop()

		util.OutputEncodedValue(ctx, pgr.Writer, value)
		fmt.Fprintln(pgr.Writer)
	} else {
		outputType(value)
		util.OutputEncodedValue(ctx, os.Stdout, value)
	}

	return 0
}

func outputType(value types.Value) {
	typeString := typeString(value)
	fmt.Fprint(os.Stdout, typeString, " - ")
}

func typeString(value types.Value) string {
	var typeString string
	switch value := value.(type) {
	case types.SerialMessage:
		switch serial.GetFileID(value) {
		case serial.StoreRootFileID:
			typeString = "StoreRoot"
		case serial.StashListFileID:
			typeString = "StashList"
		case serial.StashFileID:
			typeString = "Stash"
		case serial.TagFileID:
			typeString = "Tag"
		case serial.WorkingSetFileID:
			typeString = "WorkingSet"
		case serial.CommitFileID:
			typeString = "Commit"
		case serial.RootValueFileID:
			typeString = "RootValue"
		case serial.TableFileID:
			typeString = "Table"
		case serial.ProllyTreeNodeFileID:
			typeString = "ProllyTreeNode"
		case serial.AddressMapFileID:
			typeString = "AddressMap"
		case serial.CommitClosureFileID:
			typeString = "CommitClosure"
		case serial.TableSchemaFileID:
			typeString = "TableSchema"
		default:
			t, err := types.TypeOf(value)
			util.CheckErrorNoUsage(err)
			typeString = t.HumanReadableString()
		}
	default:
		t, err := types.TypeOf(value)
		util.CheckErrorNoUsage(err)
		typeString = t.HumanReadableString()
	}
	return typeString
}

func locationFromTimezoneArg(tz string, defaultTZ *time.Location) (*time.Location, error) {
	switch tz {
	case "local":
		return time.Local, nil
	case "utc":
		return time.UTC, nil
	case "":
		return defaultTZ, nil
	default:
		return nil, errors.New("value must be: local or utc")
	}
}
