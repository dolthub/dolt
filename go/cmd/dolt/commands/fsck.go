// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

type FsckCmd struct{}

var _ cli.Command = FsckCmd{}

func (cmd FsckCmd) Description() string {
	return "Verifies the contents of the database are not corrupted."
}

var fsckDocs = cli.CommandDocumentationContent{
	ShortDesc: "Verifies the contents of the database are not corrupted.",
	LongDesc:  "Verifies the contents of the database are not corrupted.",
	Synopsis: []string{
		"[--quiet]",
	},
}

func (cmd FsckCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(fsckDocs, cmd.ArgParser())
}

func (cmd FsckCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.QuietFlag, "", "Don't show progress. Just print final report.")

	return ap
}

func (cmd FsckCmd) Name() string {
	return "fsck"
}

// Exec re-loads the database, and verifies the integrity of all chunks in the local dolt database.
//
// We go to extra effort to load a new database because the default behavior of dolt is to self-heal for some types
// of corruption. For this reason we bypass any cached database and load a fresh one from disk.
func (cmd FsckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, _, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, fsckDocs)
	if terminate {
		return status
	}

	quiet := apr.Contains(cli.QuietFlag)

	// We expect these to work because the database has already been initialized in higher layers. We'll check anyway
	// since it's possible something went sideways or this isn't a local database.
	exists, isDir := dEnv.FS.Exists(dbfactory.DoltDataDir)
	if !exists || !isDir {
		cli.PrintErrln(fmt.Sprintf("Dolt data directory not found at %s", dbfactory.DoltDataDir))
		return 1
	}

	absPath, err := dEnv.FS.Abs(dbfactory.DoltDataDir)
	if err != nil {
		// This should never happen
		cli.PrintErrln("Could not get absolute path for dolt data directory:", err.Error())
		return 1
	}

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(absPath), os.PathSeparator)
	u, err := url.Parse(urlStr)
	if err != nil {
		panic(err)
	}

	var errs []error
	params := make(map[string]interface{})
	params[dbfactory.ChunkJournalParam] = struct{}{}
	dbFact := dbfactory.FileFactory{}
	ddb, _, _, err := dbFact.CreateDbNoCache(ctx, types.Format_Default, u, params, func(vErr error) {
		errs = append(errs, vErr)
	})
	if err != nil {
		cli.PrintErrln(fmt.Sprintf("Could not open dolt database: %s", err.Error()))
		return 1
	}
	gs, ok := datas.ChunkStoreFromDatabase(ddb).(*nbs.GenerationalNBS)
	if !ok {
		// This should never happen. Mainly a protection against future changes.
		cli.PrintErrln(fmt.Sprintf("runtime error: FSCK requires *nbs.GenerationalNBS chunk store. Got: %T", datas.ChunkStoreFromDatabase(ddb)))
		return 1
	}

	progress := make(chan string, 32)
	done := make(chan struct{})

	go func() {
		fsckHandleProgress(ctx, progress, quiet)
		close(done)
	}()

	var report *FSCKReport
	terminate = func() bool {
		defer close(progress)
		var err error
		report, err = fsckOnChunkStore(ctx, gs, errs, progress)
		if err != nil {
			// When FSCK errors, it's unexpected. As in corruption can be found and we shouldn't get an error here.
			// So we print the error and not the report.
			cli.PrintErrln(err.Error())
			return true
		}
		// skip printing the report if we were cancelled.
		select {
		case <-ctx.Done():
			cli.PrintErrln(ctx.Err().Error())
			return true
		default:
			return false
		}
	}()
	// Wait for fsckHandleProgress to finish processing all messages
	<-done

	if terminate {
		return 1
	}

	return printFSCKReport(report)
}

func printFSCKReport(report *FSCKReport) int {
	cli.Printf("Chunks Scanned: %d\n", report.ChunkCount)
	if len(report.Problems) == 0 {
		cli.Println("No problems found.")
		return 0
	} else {
		for _, e := range report.Problems {
			cli.Println(color.RedString("------ Corruption Found ------"))
			cli.PrintErrln(e.Error())
		}

		return 1
	}
}

func fsckHandleProgress(ctx context.Context, progress <-chan string, quiet bool) {
	for item := range progress {
		// when ctx is cancelled, keep draining but stop printing
		if !quiet && ctx.Err() == nil {
			cli.Println(item)
		}
	}
}

type FSCKReport struct {
	ChunkCount uint32
	Problems   []error
}

// FSCK performs a full file system check on the database. This is currently exposed with the CLI as `dolt fsck`
// The success or failure of the scan are returned in the report as a list of errors. The error returned by this function
// indicates a deeper issue such as an inability to read from the underlying storage at all.
func fsckOnChunkStore(ctx context.Context, gs *nbs.GenerationalNBS, errs []error, progress chan string) (*FSCKReport, error) {
	chunkCount, err := gs.OldGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount2, err := gs.NewGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount += chunkCount2
	proccessedCnt := int64(0)

	vs := types.NewValueStore(gs)

	decodeMsg := func(chk chunks.Chunk) string {
		hrs := ""
		val, err := types.DecodeValue(chk, vs)
		if err == nil {
			hrs = val.HumanReadableString()
		} else {
			hrs = fmt.Sprintf("Unable to decode value: %s", err.Error())
		}
		return hrs
	}

	// Append safely to the slice of errors with a mutex.
	errsLock := &sync.Mutex{}
	appendErr := func(err error) {
		errsLock.Lock()
		defer errsLock.Unlock()
		errs = append(errs, err)
	}

	// Callback for validating chunks. This code could be called concurrently, though that is not currently the case.
	validationCallback := func(chunk chunks.Chunk) {
		chunkOk := true
		pCnt := atomic.AddInt64(&proccessedCnt, 1)
		h := chunk.Hash()
		raw := chunk.Data()
		calcChkSum := hash.Of(raw)

		if h != calcChkSum {
			fuzzyMatch := false
			// Special case for the journal chunk source. We may have an address which has 4 null bytes at the end.
			if h[hash.ByteLen-1] == 0 && h[hash.ByteLen-2] == 0 && h[hash.ByteLen-3] == 0 && h[hash.ByteLen-4] == 0 {
				// Now we'll just verify that the first 16 bytes match.
				ln := hash.ByteLen - 4
				fuzzyMatch = bytes.Compare(h[:ln], calcChkSum[:ln]) == 0
			}
			if !fuzzyMatch {
				hrs := decodeMsg(chunk)
				appendErr(errors.New(fmt.Sprintf("Chunk: %s content hash mismatch: %s\n%s", h.String(), calcChkSum.String(), hrs)))
				chunkOk = false
			}
		}

		if chunkOk {
			// Round trip validation. Ensure that the top level store returns the same data.
			c, err := gs.Get(ctx, h)
			if err != nil {
				appendErr(errors.New(fmt.Sprintf("Chunk: %s load failed with error: %s", h.String(), err.Error())))
				chunkOk = false
			} else if bytes.Compare(raw, c.Data()) != 0 {
				hrs := decodeMsg(chunk)
				appendErr(errors.New(fmt.Sprintf("Chunk: %s read with incorrect ID: %s\n%s", h.String(), c.Hash().String(), hrs)))
				chunkOk = false
			}
		}

		percentage := (float64(pCnt) * 100) / float64(chunkCount)
		result := fmt.Sprintf("(%4.1f%% done)", percentage)

		progStr := "OK: " + h.String()
		if !chunkOk {
			progStr = "FAIL: " + h.String()
		}
		progStr = result + " " + progStr
		progress <- progStr
	}

	err = gs.OldGen().IterateAllChunks(ctx, validationCallback)
	if err != nil {
		return nil, err
	}
	err = gs.NewGen().IterateAllChunks(ctx, validationCallback)
	if err != nil {
		return nil, err
	}

	FSCKReport := FSCKReport{Problems: errs, ChunkCount: chunkCount}

	return &FSCKReport, nil
}
