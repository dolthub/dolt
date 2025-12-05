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
	"github.com/dolthub/dolt/go/gen/fb/serial"
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
	return "Verifies the contents of the database are not corrupted. Provides repair when possible."
}

var fsckDocs = cli.CommandDocumentationContent{
	ShortDesc: "Verifies the contents of the database are not corrupted.",
	LongDesc:  "Verifies the contents of the database are not corrupted.",
	Synopsis: []string{
		"[--quiet] [--mark-and-sweep <commit_hash>]",
		"--revive-journal-with-data-loss",
	},
}

const (
	journalReviveFlag = "revive-journal-with-data-loss"
	markAndSweepFlag  = "mark-and-sweep"
)

func (cmd FsckCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(fsckDocs, cmd.ArgParser())
}

func (cmd FsckCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.QuietFlag, "", "Don't show progress. Just print final report.")
	ap.SupportsFlag(journalReviveFlag, "", `Revives a corrupted chunk journal by discarding unparsable data.
WARNING: This may result in data loss. Your original data will be preserved in a backup file. Use this option to restore
the ability to use your Dolt database. Please contact Dolt (https://github.com/dolthub/dolt/issues) for assistance.
`)
	ap.SupportsString(markAndSweepFlag, "", "commit_hash", "Validates commit DAG and tree structures starting from the specified commit hash.")

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

	if apr.Contains(journalReviveFlag) {
		return reviveJournalWithDataLoss(dEnv)
	}

	quiet := apr.Contains(cli.QuietFlag)
	markAndSweepCommit, markAndSweep := apr.GetValue(markAndSweepFlag)

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
		if errors.Is(err, nbs.ErrJournalDataLoss) {
			cli.PrintErrln("WARNING: Chunk journal is corrupted and some data may be lost.")
			cli.PrintErrln("Run `dolt fsck --revive-journal-with-data-loss` to attempt to recover the journal by")
			cli.PrintErrln("discarding invalid data blocks. Your original data will be preserved in a backup file.")
			return 1
		} else {
			cli.PrintErrln(fmt.Sprintf("Could not open dolt database: %s", err.Error()))
		}
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
		report, err = fsckOnChunkStore(ctx, gs, errs, progress, markAndSweep, markAndSweepCommit)
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

func reviveJournalWithDataLoss(dEnv *env.DoltEnv) int {
	root, err := dEnv.FS.Abs("")
	if err != nil {
		cli.PrintErrln("Could not get absolute path for dolt data directory:", err.Error())
		return 1
	}
	noms := filepath.Join(root, ".dolt", "noms")

	path, err := nbs.ReviveJournalWithDataLoss(noms)
	if err != nil {
		cli.PrintErrln("Could not revive chunk journal:", err.Error())
		return 1
	}

	cli.Printf("Revived chunk journal at:\n%s\n", path)
	cli.Printf("For assistance recovering data, please file a ticket: https://github.com/dolthub/dolt/issues\n")
	return 0
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
		// when ctx is canceled, keep draining but stop printing
		if !quiet && ctx.Err() == nil {
			cli.Println(item)
		}
	}
}

type FSCKReport struct {
	ChunkCount uint32
	Problems   []error
}

// validateCommitDAGAndTrackChunks walks the commit DAG starting from a specific commit hash,
// validates each commit's tree structure and tracks all reachable chunks.
func validateCommitDAGAndTrackChunks(ctx context.Context, vs *types.ValueStore, startCommitHash hash.Hash, progress chan string, appendErr func(error), reachableChunks hash.HashSet) error {
	progress <- "Starting commit DAG validation..."

	// Queue for commits to process (breadth-first through commit history)
	commitQueue := []hash.Hash{startCommitHash}
	visitedCommits := make(hash.HashSet)

	for len(commitQueue) > 0 {
		commitHash := commitQueue[0]
		commitQueue = commitQueue[1:]

		// Skip if already processed
		if visitedCommits.Has(commitHash) {
			continue
		}
		visitedCommits.Insert(commitHash)
		reachableChunks.Insert(commitHash)

		progress <- fmt.Sprintf("Validating commit: %s", commitHash.String())

		// Load and validate the commit
		commitValue, err := vs.ReadValue(ctx, commitHash)
		if err != nil {
			appendErr(fmt.Errorf("failed to read commit %s: %w", commitHash.String(), err))
			continue
		}

		if commitValue == nil {
			appendErr(fmt.Errorf("commit %s not found", commitHash.String()))
			continue
		}

		// Handle SerialMessage commits only
		if serialMsg, ok := commitValue.(types.SerialMessage); ok {
			progress <- fmt.Sprintf("Processing SerialMessage commit: %s", commitHash.String())
			err = processSerialCommitAndTrack(ctx, vs, serialMsg, &commitQueue, progress, appendErr, reachableChunks)
			if err != nil {
				appendErr(fmt.Errorf("failed to process SerialMessage commit %s: %w", commitHash.String(), err))
			}
		} else {
			appendErr(fmt.Errorf("hash %s is not a SerialMessage commit, got type %T", commitHash.String(), commitValue))
			continue
		}
	}

	progress <- "Commit DAG validation completed"
	return nil
}

// processSerialCommitAndTrack handles validation and tracking of new-style SerialMessage (flatbuffer) commits
func processSerialCommitAndTrack(ctx context.Context, vs *types.ValueStore, serialMsg types.SerialMessage, commitQueue *[]hash.Hash, progress chan string, appendErr func(error), reachableChunks hash.HashSet) error {
	// Parse the SerialMessage as a commit
	var commit serial.Commit
	err := serial.InitCommitRoot(&commit, serialMsg, serial.MessagePrefixSz)
	if err != nil {
		return fmt.Errorf("failed to parse SerialMessage as commit: %w", err)
	}

	// Get the root tree hash
	rootBytes := commit.RootBytes()
	if len(rootBytes) == hash.ByteLen {
		rootHash := hash.New(rootBytes)
		progress <- fmt.Sprintf("Validating root tree: %s", rootHash.String())

		err = validateTreeAndTrack(ctx, vs, rootHash, progress, appendErr, reachableChunks)
		if err != nil {
			return fmt.Errorf("failed to validate root tree: %w", err)
		}
	} else {
		panic(fmt.Sprintf("invalid root tree length: %d", len(rootBytes)))
	}

	// Get parent commit hashes
	parentAddrs, err := types.SerialCommitParentAddrs(vs.Format(), serialMsg)
	if err != nil {
		return fmt.Errorf("failed to get parent addresses: %w", err)
	}
	for _, parentHash := range parentAddrs {
		*commitQueue = append(*commitQueue, parentHash)
	}

	// Get and track the parent closure (commit closure) reference
	parentClosureBytes := commit.ParentClosureBytes()
	if len(parentClosureBytes) == hash.ByteLen {
		parentClosureHash := hash.New(parentClosureBytes)
		reachableChunks.Insert(parentClosureHash) // Mark parent closure as reachable
		progress <- fmt.Sprintf("Tracking parent closure: %s", parentClosureHash.String())
	} else {
		panic(fmt.Sprintf("invalid parent closure length: %d", len(parentClosureBytes)))
	}

	// NM4 - TODO
	// commit.Signature()

	return nil
}

// validateTreeAndTrack performs breadth-first validation of a tree structure and tracks all reachable chunks
func validateTreeAndTrack(ctx context.Context, vs *types.ValueStore, treeHash hash.Hash, progress chan string, appendErr func(error), reachableChunks hash.HashSet) error {
	progress <- fmt.Sprintf("Validating tree: %s", treeHash.String())

	// Queue for tree entries to process (breadth-first)
	treeQueue := []hash.Hash{treeHash}
	visitedTrees := make(hash.HashSet)

	for len(treeQueue) > 0 {
		// Dequeue next tree
		currentTreeHash := treeQueue[0]
		treeQueue = treeQueue[1:]

		// Skip if already processed
		if visitedTrees.Has(currentTreeHash) {
			continue
		}
		visitedTrees.Insert(currentTreeHash)
		reachableChunks.Insert(currentTreeHash) // Mark this tree chunk as reachable

		// Load the tree
		treeValue, err := vs.ReadValue(ctx, currentTreeHash)
		if err != nil {
			return fmt.Errorf("failed to read tree %s: %w", currentTreeHash.String(), err)
		}

		if treeValue == nil {
			return fmt.Errorf("tree %s not found", currentTreeHash.String())
		}

		// Handle SerialMessage trees only
		if serialMsg, ok := treeValue.(types.SerialMessage); ok {
			progress <- fmt.Sprintf("Processing SerialMessage tree: %s", currentTreeHash.String())
			err = validateSerialTreeAndTrack(ctx, vs, serialMsg, currentTreeHash, &treeQueue, appendErr, reachableChunks)
			if err != nil {
				return fmt.Errorf("failed to validate SerialMessage tree %s: %w", currentTreeHash.String(), err)
			}
		} else {
			return fmt.Errorf("hash %s is not a SerialMessage tree, got type %T", currentTreeHash.String(), treeValue)
		}
	}

	return nil
}

// validateTree performs breadth-first validation of a tree structure
func validateTree(ctx context.Context, vs *types.ValueStore, treeHash hash.Hash, progress chan string, appendErr func(error)) error {
	progress <- fmt.Sprintf("Validating tree: %s", treeHash.String())

	// Queue for tree entries to process (breadth-first)
	treeQueue := []hash.Hash{treeHash}
	visitedTrees := make(hash.HashSet)

	for len(treeQueue) > 0 {
		// Dequeue next tree
		currentTreeHash := treeQueue[0]
		treeQueue = treeQueue[1:]

		// Skip if already processed
		if visitedTrees.Has(currentTreeHash) {
			continue
		}
		visitedTrees.Insert(currentTreeHash)

		// Load the tree
		treeValue, err := vs.ReadValue(ctx, currentTreeHash)
		if err != nil {
			return fmt.Errorf("failed to read tree %s: %w", currentTreeHash.String(), err)
		}

		if treeValue == nil {
			return fmt.Errorf("tree %s not found", currentTreeHash.String())
		}

		// Handle SerialMessage trees only
		if serialMsg, ok := treeValue.(types.SerialMessage); ok {
			progress <- fmt.Sprintf("Processing SerialMessage tree: %s", currentTreeHash.String())
			err = validateSerialTree(ctx, vs, serialMsg, currentTreeHash, &treeQueue, appendErr)
			if err != nil {
				return fmt.Errorf("failed to validate SerialMessage tree %s: %w", currentTreeHash.String(), err)
			}
		} else {
			return fmt.Errorf("hash %s is not a SerialMessage tree, got type %T", currentTreeHash.String(), treeValue)
		}
	}

	return nil
}

// validateSerialTreeAndTrack handles validation and tracking of new-style SerialMessage trees
func validateSerialTreeAndTrack(ctx context.Context, vs *types.ValueStore, serialMsg types.SerialMessage, treeHash hash.Hash, treeQueue *[]hash.Hash, appendErr func(error), reachableChunks hash.HashSet) error {
	// For now, just validate that we can read the SerialMessage without errors
	// TODO: Parse the SerialMessage as a Tree and validate its entries
	// This would require understanding the flatbuffer schema for trees

	// Basic validation: ensure the SerialMessage is valid
	if len(serialMsg) == 0 {
		return fmt.Errorf("empty SerialMessage for tree %s", treeHash.String())
	}

	// For basic validation, we'll just confirm we can read it
	// In a full implementation, we'd parse the tree entries and validate them
	// but for now this confirms the tree exists and is readable

	return nil
}

// validateSerialTree handles validation of new-style SerialMessage trees
func validateSerialTree(ctx context.Context, vs *types.ValueStore, serialMsg types.SerialMessage, treeHash hash.Hash, treeQueue *[]hash.Hash, appendErr func(error)) error {
	// For now, just validate that we can read the SerialMessage without errors
	// TODO: Parse the SerialMessage as a Tree and validate its entries
	// This would require understanding the flatbuffer schema for trees

	// Basic validation: ensure the SerialMessage is valid
	if len(serialMsg) == 0 {
		return fmt.Errorf("empty SerialMessage for tree %s", treeHash.String())
	}

	// For basic validation, we'll just confirm we can read it
	// In a full implementation, we'd parse the tree entries and validate them
	// but for now this confirms the tree exists and is readable

	return nil
}

// FSCK performs a full file system check on the database. This is currently exposed with the CLI as `dolt fsck`
// The success or failure of the scan are returned in the report as a list of errors. The error returned by this function
// indicates a deeper issue such as an inability to read from the underlying storage at all.
func fsckOnChunkStore(ctx context.Context, gs *nbs.GenerationalNBS, errs []error, progress chan string, markAndSweep bool, markAndSweepCommit string) (*FSCKReport, error) {
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

	// Build a set of all chunks found during the full scan
	allChunks := make(hash.HashSet)

	// Callback for validating chunks. This code could be called concurrently, though that is not currently the case.
	validationCallback := func(chunk chunks.Chunk) {
		chunkOk := true
		pCnt := atomic.AddInt64(&proccessedCnt, 1)
		h := chunk.Hash()
		raw := chunk.Data()
		calcChkSum := hash.Of(raw)

		// Add chunk to our set of all found chunks
		allChunks.Insert(h)

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

	// Perform commit DAG validation after full scan to identify unreachable chunks
	if markAndSweep {
		progress <- "Starting commit DAG validation..."

		// Parse the provided commit hash
		startCommitHash := hash.Parse(markAndSweepCommit)
		if startCommitHash.IsEmpty() {
			appendErr(fmt.Errorf("invalid commit hash: %s", markAndSweepCommit))
		} else {
			// Track reachable chunks by removing them from allChunks set
			reachableChunks := make(hash.HashSet)
			vs := types.NewValueStore(gs)
			err = validateCommitDAGAndTrackChunks(ctx, vs, startCommitHash, progress, appendErr, reachableChunks)
			if err != nil {
				appendErr(fmt.Errorf("commit DAG validation failed: %w", err))
			} else {
				// Report unreachable chunks (excluding essential repository infrastructure)
				unreachableCount := 0
				infrastructureCount := 0
				vs := types.NewValueStore(gs)
				for chunkHash := range allChunks {
					if !reachableChunks.Has(chunkHash) {
						// Try to read the chunk to determine if it's infrastructure
						chunkValue, err := vs.ReadValue(ctx, chunkHash)
						if err != nil || chunkValue == nil {
							// Highly suspect, as allChunks contains chunks which we loaded and verified their address.
							appendErr(fmt.Errorf("ReadValue failure to read object previously loaded %s: %w", chunkHash.String(), err))
							continue
						}

						// Check if this is essential repository infrastructure
						isInfrastructure := false
						if serialMsg, ok := chunkValue.(types.SerialMessage); ok {
							// Check for StoreRoot, WorkingSet, and AddressMap chunks using proper file ID
							id := serial.GetFileID(serialMsg)
							if id == serial.StoreRootFileID {
								isInfrastructure = true
								infrastructureCount++
								progress <- fmt.Sprintf("Infrastructure chunk: %s (type: StoreRoot)", chunkHash.String())
							} else if id == serial.WorkingSetFileID {
								isInfrastructure = true
								infrastructureCount++
								progress <- fmt.Sprintf("Infrastructure chunk: %s (type: WorkingSet)", chunkHash.String())
							} else if id == serial.AddressMapFileID {
								// AddressMap chunks are filtered out silently - don't report their existence
								isInfrastructure = true
								infrastructureCount++
							}
						}

						if !isInfrastructure {
							unreachableCount++
							humanStr, err := types.EncodedValue(ctx, chunkValue)
							progress <- fmt.Sprintf("Unreachable chunk: %s", chunkHash.String())
							progress <- fmt.Sprintf("  Type: %T", chunkValue)
							if err != nil {
								progress <- fmt.Sprintf("  Human readable: (error: %v)", err)
							} else {
								progress <- fmt.Sprintf("  Human readable: %s", humanStr)
							}

							// For SerialMessage, also try to show some raw content
							if serialMsg, ok := chunkValue.(types.SerialMessage); ok {
								if len(serialMsg) < 200 {
									progress <- fmt.Sprintf("  Raw content: %q", string(serialMsg))
								} else {
									progress <- fmt.Sprintf("  Raw content (first 200 bytes): %q", string(serialMsg[:200]))
								}
							}
						}
					}
				}
				progress <- fmt.Sprintf("Found %d unreachable chunks out of %d total chunks (%d infrastructure chunks excluded)", unreachableCount, len(allChunks), infrastructureCount)
			}
		}
	}

	FSCKReport := FSCKReport{Problems: errs, ChunkCount: chunkCount}

	return &FSCKReport, nil
}
