// Copyright 2021 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
)

const numFilesParam = "numfiles"

type RootsCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RootsCmd) Name() string {
	return "roots"
}

// Hidden should return true if this command should be hidden from the help text
func (cmd RootsCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd RootsCmd) RequiresRepo() bool {
	return false
}

// Description returns a description of the command
func (cmd RootsCmd) Description() string {
	return "Displays the current Dolt cli version."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd RootsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func (cmd RootsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsInt(numFilesParam, "n", "number", "Number of table files to scan.")
	return ap
}

// Exec executes the command
func (cmd RootsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	dir := filepath.Join(dEnv.GetDoltDir(), dbfactory.DataDir)
	itr, err := NewTableFileIter(dir, dEnv.FS)

	if err != nil {
		return BuildVerrAndExit("Unable to read table files.", err)
	}

	n := apr.GetIntOrDefault(numFilesParam, len(itr.files))
	for i := 0; i < n; i++ {
		fPath, modified := itr.next()
		err = cmd.processTableFile(ctx, fPath, modified, dEnv.FS)

		if err == io.EOF {
			break
		} else if err != nil {
			cli.Println(color.YellowString("Failed to process '%s'. Cause: %v", fPath, err))
		}
	}

	return 0
}

func (cmd RootsCmd) processTableFile(ctx context.Context, path string, modified time.Time, fs filesys.Filesys) error {
	cli.Printf("Processing '%s' last modified: %v\n", path, modified)
	rdCloser, err := fs.OpenForRead(path)

	if err != nil {
		return err
	}

	defer rdCloser.Close()

	return nbs.IterChunks(rdCloser.(io.ReadSeeker), func(chunk chunks.Chunk) (stop bool, err error) {
		//Want a clean db every loop
		sp, _ := spec.ForDatabase("mem")
		db := sp.GetDatabase(ctx)

		value, err := types.DecodeValue(chunk, db)

		if err != nil {
			return false, err
		}

		if m, ok := value.(types.Map); ok && types.IsMapLeaf(m) {
			mightBeDatasetMap := true
			_ = m.IterAll(ctx, func(key, value types.Value) error {
				kStr, kOK := key.(types.String)
				vIsRef := value.Kind() == types.RefKind

				if !kOK || !vIsRef || !(ref.IsRef(string(kStr)) || strings.HasPrefix(string(kStr), "tmp/")) {
					mightBeDatasetMap = false
					return io.EOF
				}

				return nil
			})

			if mightBeDatasetMap {
				err := types.WriteEncodedValue(ctx, cli.OutStream, value)

				if err != nil {
					return false, err
				}

				cli.Println()
			}
		}

		return false, nil
	})
}

type fileAndTime struct {
	Path     string
	Modified time.Time
}

type TableFileIter struct {
	files []fileAndTime
	pos   int
}

func NewTableFileIter(dir string, fs filesys.Filesys) (*TableFileIter, error) {
	var tableFiles []fileAndTime
	err := fs.Iter(dir, false, func(path string, size int64, isDir bool) (stop bool) {
		if !isDir {
			filename := filepath.Base(path)

			if len(filename) == 32 {
				t, ok := fs.LastModified(path)
				if !ok {
					t = time.Now()
				}

				tableFiles = append(tableFiles, fileAndTime{path, t})
			}
		}

		return false
	})

	if err != nil {
		return nil, err
	} else if len(tableFiles) == 0 {
		return nil, fmt.Errorf("No table files found in '%s'", dir)
	}

	sort.Slice(tableFiles, func(i, j int) bool {
		return tableFiles[i].Modified.Sub(tableFiles[j].Modified) > 0
	})

	return &TableFileIter{tableFiles, 0}, nil
}

func (itr *TableFileIter) next() (string, time.Time) {
	curr := itr.files[itr.pos]
	itr.pos++

	return curr.Path, curr.Modified
}
