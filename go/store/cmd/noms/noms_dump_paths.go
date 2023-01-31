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

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var nomsWalk = &util.Command{
	Run:       runWalk,
	UsageLine: "walk [flags] <object>",
	Short:     "Prints a depth-first listing of all paths to leaf data, beginning with the reference provided. If no ref is provided, uses the root.",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupWalkFlags,
	Nargs:     1,
}

func setupWalkFlags() *flag.FlagSet {
	walkPathSet := flag.NewFlagSet("walk", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(walkPathSet)
	verbose.RegisterVerboseFlags(walkPathSet)
	return walkPathSet
}

func runWalk(ctx context.Context, args []string) int {
	cfg := config.NewResolver()

	var value types.Value
	// TODO: default to manifest root
	startHash := args[0]
	database, _, value, err := cfg.GetPath(ctx, startHash)

	if err != nil {
		util.CheckErrorNoUsage(err)
	} else {
	}

	defer database.Close()

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", startHash)
		return 0
	}
	
	if showPages {
		pgr := outputpager.Start()
		defer pgr.Stop()

		err := walkAddrsSimple(ctx, pgr.Writer, startHash, value, cfg)
		if err != nil {
			fmt.Fprintf(pgr.Writer, "error encountered: %s", err.Error())
		}
		fmt.Fprintln(pgr.Writer)
	} else {
		err := walkAddrsSimple(ctx, os.Stdout, startHash, value, cfg)
		if err != nil {
			if err != nil {
				fmt.Fprintf(os.Stdout, "error encountered: %s", err.Error())
			}
		}
		fmt.Fprintln(os.Stdout)
	}

	return 0
}

func walkAddrs(ctx context.Context, w io.Writer, path string, value types.Value, cfg *config.Resolver) error {
	// Begin by augmenting our path with the type of this value. At every step we walk, we will add a pair of hash, type
	path += "(" + serialType(value) + ")"
	
	walk := func(addr hash.Hash) error {
		_, _, value, err := cfg.GetPath(ctx, ".dolt/noms::#" + addr.String())

		if err != nil {
			return err
		}
		
		fmt.Fprintf(w, "%s / #%s (%s)\n", path, addr.String(), serialType(value))
		
		if value == nil {
			return fmt.Errorf("Error: %s not found for path %s", addr.String(), path)
		}
		
		return walkAddrs(ctx, w, path + " > " + addr.String(), value, cfg)
	}
	
	switch msg := value.(type) {
	// Some types of serial message need to be output here because of dependency cycles between types / tree package
	case types.SerialMessage:
		id := serial.GetFileID(msg)
		switch id {
		case serial.ProllyTreeNodeFileID:
			var pm serial.ProllyTreeNode
			err := serial.InitProllyTreeNodeRoot(&pm, msg, serial.MessagePrefixSz)
			if err != nil {
				return err
			}
			
			isLeaf := pm.AddressArrayLength() == 0 && pm.ValueAddressOffsetsLength() == 0
			if isLeaf {
				fmt.Fprintf(w, "%s\n", path)
			}
		case serial.MergeArtifactsFileID:
			var ma serial.MergeArtifacts
			err := serial.InitMergeArtifactsRoot(&ma, msg, serial.MessagePrefixSz)
			if err != nil {
				return err
			}

			isLeaf := ma.AddressArrayLength() == 0 && ma.KeyAddressOffsetsLength() == 0
			if isLeaf {
				fmt.Fprintf(w, "OUTPUT: %s\n", path)
			}
		case serial.CommitClosureFileID:
			m := serial.GetRootAsCommitClosure(msg, serial.MessagePrefixSz)
			isLeaf := m.AddressArrayLength() == 0 && m.TreeLevel() != 0

			if isLeaf {
				fmt.Fprintf(w, "OUTPUT: %s\n", path)
			}
		case serial.BlobFileID:
			var b serial.Blob
			err := serial.InitBlobRoot(&b, msg, serial.MessagePrefixSz)
			if err != nil {
				return err
			}
			
			if b.AddressArrayLength() == 0 {
				fmt.Fprintf(w, "OUTPUT: %s\n", path)
			}
		case serial.TableSchemaFileID, serial.ForeignKeyCollectionFileID:
			// these are leaf nodes
			fmt.Fprintf(w, "OUTPUT: %s\n", path)
			return nil
		}
		
		return msg.WalkAddrs(types.Format_Default, walk)

	default:
		// skip non serial values
	}
	return nil
}

var seenMessages = hash.NewHashSet()

func walkAddrsSimple(ctx context.Context, w io.Writer, path string, value types.Value, cfg *config.Resolver) error {
	walk := func(addr hash.Hash) error {
		_, _, value, err := cfg.GetPath(ctx, ".dolt/noms::#" + addr.String())

		if err != nil {
			return err
		}

		if value == nil {
			return fmt.Errorf("Dangling reference: hash %s not found for path %s", addr.String(), path)
		}

		newPath := fmt.Sprintf("%s > %s(%s)", path, addr.String(), serialType(value))
		fmt.Fprintf(w, "%s\n", newPath)
		
		// We only want to recurse on messages we haven't seen before. This means not outputting some possible paths to 
		// some chunks, but since there are so very many paths to a typical chunk this is a huge time saver.
		if !seenMessages.Has(addr) {
			seenMessages.Insert(addr)
			return walkAddrsSimple(ctx, w, newPath, value, cfg)
		}
		
		return nil
	}

	switch msg := value.(type) {
	// Some types of serial message need to be output here because of dependency cycles between types / tree package
	case types.SerialMessage:
		return msg.WalkAddrs(types.Format_Default, walk)
	default:
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
