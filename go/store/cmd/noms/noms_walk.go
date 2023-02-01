// Copyright 2022 Dolthub, Inc.
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

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var nomsWalk = &util.Command{
	Run:       runWalk,
	UsageLine: "walk [flags] [<object>]",
	Short:     "Prints a depth-first listing of all paths to leaf data, beginning with the reference provided. If no ref is provided, uses the manifest root.",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupWalkFlags,
	Nargs:     0,
}

var (
	quiet = false
)

func setupWalkFlags() *flag.FlagSet {
	walkPathSet := flag.NewFlagSet("walk", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(walkPathSet)
	verbose.RegisterVerboseFlags(walkPathSet)
	walkPathSet.BoolVar(&quiet, "quiet", false, "If true do not print all ref paths, only dangling refs")
	return walkPathSet
}

func runWalk(ctx context.Context, args []string) int {
	cfg := config.NewResolver()

	var value types.Value

	var startHash string
	if len(args) < 1 {
		manifestReader, err := os.Open("./.dolt/noms/manifest")
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading manifest: ", err)
			return 1
		}

		manifest, err := nbs.ParseManifest(manifestReader)
		d.PanicIfError(err)

		startHash = manifest.GetRoot().String()
	} else {
		startHash = args[0]
	}

	fullPath := startHash

	if strings.HasPrefix(fullPath, "#") && !strings.HasPrefix(fullPath, ".dolt/noms::#") {
		fullPath = ".dolt/noms::" + fullPath
	} else if !strings.HasPrefix(fullPath, ".dolt/noms::#") {
		fullPath = ".dolt/noms::#" + fullPath
	}

	database, vrw, value, err := cfg.GetPath(ctx, fullPath)

	if err != nil {
		util.CheckErrorNoUsage(err)
	} else {
	}

	defer database.Close()

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", fullPath)
		return 0
	}

	if showPages {
		pgr := outputpager.Start()
		defer pgr.Stop()

		err := walkAddrs(ctx, pgr.Writer, startHash, value, vrw)
		if err != nil {
			fmt.Fprintf(pgr.Writer, "error encountered: %s", err.Error())
		}
		fmt.Fprintln(pgr.Writer)
	} else {
		err := walkAddrs(ctx, os.Stdout, startHash, value, vrw)
		if err != nil {
			if err != nil {
				fmt.Fprintf(os.Stdout, "error encountered: %s", err.Error())
			}
		}
		fmt.Fprintln(os.Stdout)
	}

	return 0
}

var seenMessages = hash.NewHashSet()
var numProcessed = 0

func walkAddrs(ctx context.Context, w io.Writer, path string, value types.Value, cfg types.ValueReadWriter) error {
	walk := func(addr hash.Hash) error {
		value, err := cfg.ReadValue(ctx, addr)

		if err != nil {
			return err
		}

		if value == nil {
			fmt.Fprintf(w, "Dangling reference: hash %s not found for path %s\n", addr.String(), path)
			return nil
		}

		numProcessed++

		newPath := fmt.Sprintf("%s > %s(%s)", path, addr.String(), serialType(value))
		if !quiet {
			fmt.Fprintf(w, "%s\n", newPath)
		}

		if numProcessed%100_000 == 0 {
			fmt.Fprintf(os.Stderr, "%d refs walked\n", numProcessed)
		}

		// We only want to recurse on messages we haven't seen before. This means not outputting some possible paths to
		// some chunks, but since there are so very many paths to a typical chunk this is a huge time saver.
		if !seenMessages.Has(addr) {
			seenMessages.Insert(addr)
			return walkAddrs(ctx, w, newPath, value, cfg)
		}

		return nil
	}

	switch msg := value.(type) {
	case types.SerialMessage:
		return msg.WalkAddrs(types.Format_Default, walk)
	default:
		// non-serial values can't be walked
		return nil
	}
}

func serialType(value types.Value) string {
	sm, ok := value.(types.SerialMessage)
	if !ok {
		return typeString(value)
	}

	return serial.GetFileID(sm)
}
