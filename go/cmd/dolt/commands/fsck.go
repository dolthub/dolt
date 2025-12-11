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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
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
		"[--quiet]",
		"--revive-journal-with-data-loss",
	},
}

const (
	journalReviveFlag = "revive-journal-with-data-loss"
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
		report, err = fsckOnChunkStore(ctx, dEnv.DoltDB(ctx), gs, errs, progress)
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

// validateCommitDAGAndTrackChunks walks the commit DAG starting from multiple commit hashes,
// validates each commit's tree structure and tracks all reachable chunks.
func validateCommitDAGAndTrackChunks(ctx context.Context, vs *types.ValueStore, startCommitHashes []hash.Hash, progress chan string, appendErr func(error), reachableChunks *hash.HashSet) error {
	progress <- "Starting commit DAG validation..."
	// Queue for commits to process (breadth-first from all heads through commit history)
	commitQueue := make([]hash.Hash, len(startCommitHashes))
	copy(commitQueue, startCommitHashes)
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
			appendErr(fmt.Errorf("commit object for %s not found (no err)", commitHash.String()))
			continue
		}

		if serialMsg, ok := commitValue.(types.SerialMessage); ok {
			progress <- fmt.Sprintf("Processing SerialMessage commit: %s", commitHash.String())
			err = processSerialCommitAndTrack(ctx, commitHash, vs, serialMsg, &commitQueue, progress, appendErr, reachableChunks)
			if err != nil {
				appendErr(fmt.Errorf("failed to process SerialMessage commit %s: %w", commitHash.String(), err))
			}
		} else {
			// Spit on the old format.
			panic(fmt.Sprintf("hash %s is not a SerialMessage commit, got type %T", commitHash.String(), commitValue))
		}
	}

	progress <- "Commit DAG validation completed"
	return nil
}

// processSerialCommitAndTrack handles validation and tracking of new-style SerialMessage (flatbuffer) commits
func processSerialCommitAndTrack(
	ctx context.Context,
	commitHash hash.Hash, // Use just for error messages.
	vs *types.ValueStore,
	serialMsg types.SerialMessage,
	commitQueue *[]hash.Hash,
	progress chan string,
	appendErr func(error),
	reachableChunks *hash.HashSet,
) error {
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

		err = validateTreeAndTrack(ctx, vs, commitHash, rootHash, progress, appendErr, reachableChunks)
		if err != nil {
			// Any Errors here are unexpected. appendErr should be used for things we expect to possibly fail.
			return fmt.Errorf("failed to validate root tree %s: %w", rootHash.String(), err)
		}
	} else {
		panic(fmt.Sprintf("invalid root tree length: %d", len(rootBytes)))
	}

	// Enqueue parent commits for processing
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
		if !parentClosureHash.IsEmpty() {
			reachableChunks.Insert(parentClosureHash) // Mark parent closure as reachable

			// Closure will be a single object
			value, err := vs.ReadValue(ctx, parentClosureHash)
			if err != nil {
				appendErr(fmt.Errorf("Commit %s is missing data. Failed to read commit closure %s: %w", commitHash.String(), parentClosureHash.String(), err))
			} else if value == nil {
				appendErr(fmt.Errorf("Commit %s is missing data. Failed to read commit closure %s", commitHash.String(), parentClosureHash.String()))
			} else {
				// NM4 - TODO: Validate closure contents. We should probably do this after we walk the graph,
				// then confirm all parents were seen.
				// progress <- fmt.Sprintf("Found commit closure: %s", parentClosureHash.String())
			}
		} else if len(parentAddrs) != 0 {
			// Empty closure should happen only for root commits. Make sure there are no parents.
			appendErr(fmt.Errorf("commit %s has empty parent closure but has %d parents", commitHash.String(), len(parentAddrs)))
		}

	} else {
		panic(fmt.Sprintf("invalid parent closure length: %d", len(parentClosureBytes)))
	}

	// NM4 - TODO: Validate signatures. Require public keys. Do we do this anywhere?
	// commit.Signature()

	return nil
}

// validateTreeAndTrack performs breadth-first validation of a tree structure and tracks all reachable chunks
func validateTreeAndTrack(
	ctx context.Context,
	vs *types.ValueStore,
	commitHash, // Use just for error messages.
	treeHash hash.Hash,
	progress chan string,
	appendErr func(error),
	reachableChunks *hash.HashSet,
) error {

	progress <- fmt.Sprintf("Validating commit %s's data tree: %s", commitHash.String(), treeHash.String())

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
		reachableChunks.Insert(currentTreeHash)

		// Load the tree
		treeValue, err := vs.ReadValue(ctx, currentTreeHash)
		if err != nil || treeValue == nil {
			appendErr(fmt.Errorf("failed to read tree: %w", err))
			continue
		}

		// Handle SerialMessage trees only
		if serialMsg, ok := treeValue.(types.SerialMessage); ok {
			id := serial.GetFileID(serialMsg)
			progress <- fmt.Sprintf("Processing tree object (%s): %s", id, currentTreeHash.String())
			err = validateSerialTreeAndTrack(ctx, vs, serialMsg, commitHash, currentTreeHash, &treeQueue, appendErr, reachableChunks)
			if err != nil {
				return fmt.Errorf("failed to validate tree object %s: %w", currentTreeHash.String(), err)
			}
		} else {
			// Spit on the old format.
			return fmt.Errorf("hash %s is not a SerialMessage tree, got type %T", currentTreeHash.String(), treeValue)
		}
	}

	return nil
}

// validateSerialTreeAndTrack handles validation of SerialMessages which are trees of objects. We want to track all reachable chunks,
// and any errors encountered during traversal are appended via appendErr. If this function returns an error, it indicates an unexpected failure,
// and further processing should halt.
func validateSerialTreeAndTrack(
	ctx context.Context,
	vs *types.ValueStore,
	serialMsg types.SerialMessage,
	commitHash hash.Hash,
	treeHash hash.Hash,
	treeQueue *[]hash.Hash,
	appendErr func(error),
	reachableChunks *hash.HashSet,
) error {
	if len(serialMsg) < serial.MessagePrefixSz {
		return fmt.Errorf("empty SerialMessage for tree %s", treeHash.String())
	}

	// Use the types system to walk all addresses in this SerialMessage
	// This is similar to how SaveHashes works - traverse all reachable chunks
	err := serialMsg.WalkAddrs(vs.Format(), func(addr hash.Hash) error {
		if !reachableChunks.Has(addr) {
			reachableChunks.Insert(addr)

			// Try to load the referenced value to continue traversal
			refValue, readErr := vs.ReadValue(ctx, addr)
			if readErr != nil {

				appendErr(fmt.Errorf("commit::%s: tree %s -> (missing) %s: %w", commitHash.String(), treeHash.String(), addr.String(), readErr))
				return nil // Continue walking other addresses
			}
			if refValue == nil {
				appendErr(fmt.Errorf("commit::%s: tree %s -> (missing) %s", commitHash.String(), treeHash.String(), addr.String()))
				return nil
			}

			// If it's another serialized structure, add to queue for further processing
			if _, isSerial := refValue.(types.SerialMessage); isSerial {
				*treeQueue = append(*treeQueue, addr)
			} else {
				// This should never happen.
				panic(fmt.Sprintf("commit::%s: referenced chunk %s from tree %s is not a SerialMessage, got type %T", commitHash.String(), addr.String(), treeHash.String(), refValue))
			}
		}
		return nil
	})

	if err != nil {
		// We intentionally never return errors from WalkAddrs, so any error here is unexpected. Halt.
		return fmt.Errorf("failed to walk references in tree %s: %w", treeHash.String(), err)
	}

	return nil
}

// FSCK performs a full file system check on the database. This is currently exposed with the CLI as `dolt fsck`
// The success or failure of the scan are returned in the report as a list of errors. The error returned by this function
// indicates a deeper issue such as an inability to read from the underlying storage at all.
func fsckOnChunkStore(ctx context.Context, ddb *doltdb.DoltDB, gs *nbs.GenerationalNBS, errs []error, progress chan string) (*FSCKReport, error) {
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

	// Perform commit DAG validation from all branch HEADs and tags to identify unreachable chunks
	progress <- "--------------- All Objects scanned. Starting commit DAG validation ---------------"

	references := make(map[hash.Hash][]string)

	// helper to get refs, resolve them to commits, and add them to startCommits/references.
	// The callback is expected to work, we'll error out if it doesn't. resolving commits is less certain though,
	// so those errors are appended via appendErr to be reported at the end.
	refGetter := func(cb func(context.Context) ([]ref.DoltRef, error)) error {
		refs, err := cb(ctx)
		if err != nil {
			return fmt.Errorf("failed to get references: %w", err)
		}
		for _, ref := range refs {
			head, err := ddb.ResolveCommitRef(ctx, ref)
			if err != nil {
				appendErr(fmt.Errorf("failed to resolve reference %s: %w", ref.GetPath(), err))
				continue
			}
			commitHash, err := head.HashOf()
			if err != nil {
				appendErr(fmt.Errorf("failed to get hash for refeence %s: %w", ref.GetPath(), err))
				continue
			}
			references[commitHash] = append(references[commitHash], ref.GetPath())
		}
		return nil
	}
	err = refGetter(ddb.GetBranches)
	if err != nil {
		return nil, fmt.Errorf("failed to get branches: %w", err)
	}
	err = refGetter(ddb.GetRemoteRefs)
	if err != nil {
		return nil, fmt.Errorf("failed to get remote branches: %w", err)
	}
	err = refGetter(ddb.GetTags)
	if err != nil {
		return nil, fmt.Errorf("failed to get tags: %w", err)
	}

	startCommits := make([]hash.Hash, 0, len(references))
	for commitHash := range references {
		startCommits = append(startCommits, commitHash)
	}

	// NM4 - TODO: Get working sets too. Currently commit dag walking will report
	// uncommited working set chunks as unreachable.

	if len(startCommits) > 0 {
		progress <- fmt.Sprintf("Starting commit DAG validation from %d local branches, remote branches, and tags...", len(startCommits))

		reachedChunks := make(hash.HashSet)

		err = validateCommitDAGAndTrackChunks(ctx, vs, startCommits, progress, appendErr, &reachedChunks)
		if err != nil {
			appendErr(fmt.Errorf("commit DAG validation failed: %w", err))
		}

		// Report unreachable chunks grouped by message type and count. NM4 - include size.
		typeMap := make(map[string]int)
		unreachableCount := 0
		for chunkHash := range allChunks {
			if !reachedChunks.Has(chunkHash) {
				// Try to read the chunk to determine it's type. We'll summarize.
				chunkValue, err := vs.ReadValue(ctx, chunkHash)
				if err != nil || chunkValue == nil {
					// Highly suspect, as allChunks contains chunks which we loaded and verified their address.
					appendErr(fmt.Errorf("ReadValue failure to read object previously loaded %s: %w", chunkHash.String(), err))
					continue
				}

				if serialMsg, ok := chunkValue.(types.SerialMessage); ok {
					id := serial.GetFileID(serialMsg)
					typeMap[id]++
				} else {
					// Spit on the old format.
					panic(fmt.Sprintf("hash %s is not a SerialMessage, got type %T", chunkHash.String(), chunkValue))
				}
			}
		}

		for t, count := range typeMap {
			progress <- fmt.Sprintf("Unreachable chunks of type %s: %d", t, count)
			unreachableCount += count
		}

		progress <- fmt.Sprintf("Found %d unreachable chunks out of %d total chunks)", unreachableCount, len(allChunks))
	} else {
		progress <- "No branches, remote branches, or tags found - skipping commit DAG validation"
	}

	FSCKReport := FSCKReport{Problems: errs, ChunkCount: chunkCount}

	return &FSCKReport, nil
}
