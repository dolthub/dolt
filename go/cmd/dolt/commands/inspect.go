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
	"math"
	"path/filepath"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
)

const tableFileIndexFlag = "index"

type InspectCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd InspectCmd) Name() string {
	return "inspect"
}

// Hidden should return true if this command should be hidden from the help text
func (cmd InspectCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd InspectCmd) RequiresRepo() bool {
	return true
}

// Description returns a description of the command
func (cmd InspectCmd) Description() string {
	return "Inspects a Dolt Database and collects stats."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd InspectCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	return nil
}

func (cmd InspectCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(tableFileIndexFlag, "i", "Measure distribution error in table file chunk indexes.")
	return ap
}

// Exec executes the command
func (cmd InspectCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError
	if apr.Contains(tableFileIndexFlag) {
		verr = cmd.measureChunkIndexDistribution(ctx, dEnv)
	}

	return HandleVErrAndExitCode(verr, usage)
}

func (cmd InspectCmd) measureChunkIndexDistribution(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	newGen := filepath.Join(dEnv.GetDoltDir(), dbfactory.DataDir)
	oldGen := filepath.Join(newGen, "oldgen")

	itr, err := NewTableFileIter([]string{newGen, oldGen}, dEnv.FS)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	sumErr, sumCnt := 0.0, 0
	for {
		path, _ := itr.next()
		if path == "" {
			break
		}

		summary, err := cmd.processTableFile(path, dEnv.FS)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		sumErr += summary.sumErr
		sumCnt += int(summary.count)

		cli.Println(summary.format())
	}
	cli.Printf("average guess error: %f", sumErr/float64(sumCnt))

	return nil
}

func (cmd InspectCmd) processTableFile(path string, fs filesys.Filesys) (sum *chunkIndexSummary, err error) {
	var rdr io.ReadCloser
	rdr, err = fs.OpenForRead(path)
	if err != nil {
		return sum, err
	}
	defer func() {
		cerr := rdr.Close()
		if err == nil {
			err = cerr
		}
	}()

	var prefixes []uint64
	prefixes, err = nbs.GetTableIndexPrefixes(rdr.(io.ReadSeeker))
	if err != nil {
		return sum, err
	}

	sum = &chunkIndexSummary{
		file:  path,
		count: uint32(len(prefixes)),
		//errs:  make([]float64, 0, len(prefixes)),
	}

	for i, prefix := range prefixes {
		sum.addPrefix(i, prefix)
	}
	return
}

type chunkIndexSummary struct {
	file  string
	count uint32
	//errs   []float64
	sumErr float64
	maxErr float64
}

func (s *chunkIndexSummary) format() string {
	return fmt.Sprintf("file: %s \t count: %d sum error: %f \t max error: %f ",
		s.file, s.count, s.sumErr, s.maxErr)
}

func (s *chunkIndexSummary) addPrefix(i int, prefix uint64) {
	g := nbs.GuessPrefixOrdinal(prefix, s.count)
	guessErr := math.Abs(float64(i - g))

	//s.errs = append(s.errs, guessErr)
	s.sumErr += guessErr
	if guessErr > s.maxErr {
		s.maxErr = guessErr
	}
}
