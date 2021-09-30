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

package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"

	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/spec"
)

var nomsManifest = &util.Command{
	Run:       runManifest,
	UsageLine: "manifest <db-spec>",
	Short:     "Get or set the current root hash of the entire database",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.",
	Flags:     setupManifestFlags,
	Nargs:     1,
}

type NbsFile struct {
	manifestSpec nbs.TableSpecInfo
	fileInfo     os.FileInfo
	fileInfoErr  error
}

func (f NbsFile) sizeStr() string {
	if f.fileInfoErr == nil {
		bi := big.Int{}
		bi.SetInt64(f.fileInfo.Size())
		return humanize.BigBytes(&bi)
	}

	return "-"
}

func (f NbsFile) modTimeStr() string {
	if f.fileInfoErr == nil {
		return f.fileInfo.ModTime().String()[:22]
	}

	return "-"
}

func setupManifestFlags() *flag.FlagSet {
	flagSet := flag.NewFlagSet("manifest", flag.ExitOnError)
	return flagSet
}

func runManifest(ctx context.Context, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Not enough arguments")
		return 0
	}

	dbArg := args[0]
	spec, err := spec.ForDatabase(dbArg)

	if err != nil {
		fmt.Fprintln(os.Stderr, args[0]+" is not a valid database spec")
		return 1
	}

	if spec.Protocol != "nbs" {
		fmt.Fprintln(os.Stderr, spec.Protocol+" databases not supported by this command yet.  Only nbs")
		return 1
	}

	manifestFile := filepath.Join(spec.DatabaseName, "manifest")
	manifestReader, err := os.Open(manifestFile)

	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not read file", manifestFile, err)
		return 1
	}

	manifest, err := nbs.ParseManifest(manifestReader)
	d.PanicIfError(err)

	numSpecs := manifest.NumTableSpecs()
	nbsFiles := make([]NbsFile, numSpecs)
	for i := 0; i < numSpecs; i++ {
		tableSpecInfo := manifest.GetTableSpecInfo(i)
		path := filepath.Join(spec.DatabaseName, tableSpecInfo.GetName())
		fileInfo, err := os.Stat(path)
		nbsFiles[i] = NbsFile{tableSpecInfo, fileInfo, err}
	}

	// Sort these by time stamp makes it much easier to see what happens over time and understand
	// what is going on as you run different operations.
	sort.SliceStable(nbsFiles, func(i, j int) bool {
		f1Stat, err1 := nbsFiles[i].fileInfo, nbsFiles[i].fileInfoErr
		f2Stat, err2 := nbsFiles[j].fileInfo, nbsFiles[j].fileInfoErr

		if err1 != nil {
			return true
		} else if err2 != nil {
			return false
		}

		return f1Stat.ModTime().Sub(f2Stat.ModTime()) < 0
	})

	fmt.Println(manifestFile + ":")
	fmt.Printf("    version: %s\n", manifest.GetVersion())
	fmt.Printf("    lock:    %s\n", manifest.GetLock())
	fmt.Printf("    root:    %s\n", manifest.GetRoot())
	fmt.Println("    referenced nbs files:")

	for _, nbsFile := range nbsFiles {
		name := nbsFile.manifestSpec.GetName()
		chunkCnt := nbsFile.manifestSpec.GetChunkCount()
		sizeStr := nbsFile.sizeStr()
		existsStr := nbsFile.fileInfoErr == nil
		modTimeStr := nbsFile.modTimeStr()
		fmt.Printf("        %s  chunks: %2d  exists: %-6t  size: %7s  modified: %10s\n", name, chunkCnt, existsStr, sizeStr, modTimeStr)
	}

	return 0
}
