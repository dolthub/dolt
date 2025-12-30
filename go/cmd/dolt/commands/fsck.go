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

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
	// Information is presented to users in two forms: progress messages, and an error report. The progress messages are sent
	// over a channel to a separate goroutine that handles printing them (so that progress can be reported while fsck is running).
	// The error report is built up in a slice of errors that is passed around and appended to as issues are found.
	progress := make(chan string, 32)
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
		// This should never happen. Mainly protection against future changes.
		cli.PrintErrln(fmt.Sprintf("runtime error: FSCK requires *nbs.GenerationalNBS chunk store. Got: %T", datas.ChunkStoreFromDatabase(ddb)))
		return 1
	}

	done := make(chan struct{})

	go func() {
		fsckHandleProgress(ctx, progress, quiet)
		close(done)
	}()

	var report *FSCKReport
	terminate = func() bool {
		defer close(progress)
		var err error
		report, err = fsckOnChunkStore(ctx, gs, &errs, progress)
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
	if len(*(report.Problems)) == 0 {
		cli.Println("No problems found.")
		return 0
	} else {
		for _, e := range *(report.Problems) {
			cli.Println(color.RedString("------ Corruption Found ------"))
			cli.Println(e.Error())
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
	Problems   *[]error
}

// FSCK performs a full file system check on the database. This is currently exposed with the CLI as `dolt fsck`
// The success or failure of the scan are returned in the report as a list of errors. The error returned by this function
// indicates a deeper issue such as an inability to read from the underlying storage at all.
//
// The FSCK process runs in multiple phases:
//  1. Full chunk scan: Every chunk in the store is read, its hash is verified, and a round-trip load is performed.
//     During this phase, we also build a set of all chunks found, and categorize them by their message type. In particular,
//     we identify commit objects for later processing.
//  2. Commit DAG walking: Starting from all branch HEADs and tags, we walk the commit DAG to identify reachable commits.
//     This phase is lightweight and only validates commit objects and their parent relationships.
//  3. Commit tree validation: For each commit found in phase 2, we validate its tree structure and all referenced objects.
//
// NM4 - review when done.....
func fsckOnChunkStore(ctx context.Context, gs *nbs.GenerationalNBS, errs *[]error, progress chan string) (*FSCKReport, error) {
	appendErr := func(err error) {
		*errs = append(*errs, err)
	}

	rt, err := newRoundTripper(gs, progress, appendErr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize FSCK round tripper: %w", err)
	}
	err = rt.scanAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed during full chunk scan: %w", err)
	}

	chunkCount := rt.chunkCount
	chunksByType := rt.chunksByType

	// Report chunk type summary
	progress <- "--------------- Chunk Type Summary ---------------"
	for chunkType, chunks := range chunksByType {
		progress <- fmt.Sprintf("Found %d chunks of type: %s", len(chunks), chunkType)
	}

	// Perform commit DAG validation from all branch HEADs and tags to identify unreachable chunks
	progress <- "--------------- All Objects scanned. Starting commit validation ---------------"

	// Find all commit objects from our scanned chunks
	allCommitsSet := make(hash.HashSet)
	if commitChunks, hasCommits := chunksByType[serial.CommitFileID]; hasCommits {
		for _, commitHash := range commitChunks {
			allCommitsSet.Insert(commitHash)
		}
		progress <- fmt.Sprintf("Found %d commit objects", len(allCommitsSet))
	} else {
		progress <- "No commit objects found during chunk scan"
	}

	reachableCommits, err := walkCommitDAGFromRefs(ctx, gs, &allCommitsSet, progress, appendErr)
	if err != nil {
		// NM4 - hmmm. Not sure if we should fail here. TBD.
		appendErr(fmt.Errorf("commit DAG walking failed: %w", err))
	}

	// Phase 3: Tree validation for commits (performance heavy)
	if len(reachableCommits) > 0 {
		progress <- fmt.Sprintf("Starting tree validation for %d commit objects...", len(reachableCommits))

		vs := types.NewValueStore(gs)

		commitReachableChunks, err := validateTreeAndTrackChunks(ctx, vs, &reachableCommits, progress, appendErr)
		if err != nil {
			// NM4 - hmmm. Not sure if we should fail here. TBD.
			appendErr(fmt.Errorf("commit validation failed: %w", err))
		}

		// Report which commits are reachable vs unreachable from branches/tags. NM4. easier/cleaner way?
		unreachableCommits := 0
		for commitHash := range allCommitsSet {
			if !reachableCommits.Has(commitHash) {
				unreachableCommits++
			}
		}
		unreachableChunks := chunkCount - uint32(commitReachableChunks.Size())

		progress <- fmt.Sprintf("Found %d unreachable commits (not reachable from any branch/tag)", unreachableCommits)
		progress <- fmt.Sprintf("Validated %d chunks reachable by branches and tags (unreachable: %d)", commitReachableChunks.Size(), unreachableChunks)
	} else {
		progress <- "No commit objects found - skipping tree validation"
	}

	FSCKReport := FSCKReport{Problems: errs, ChunkCount: chunkCount}

	return &FSCKReport, nil
}

// roundTripper performs a full scan of chunks, verifying that their hashes match their content. It also collects
// all chunks found, categorized by their message type.
type roundTripper struct {
	vs            *types.ValueStore
	gs            *nbs.GenerationalNBS
	chunkCount    uint32
	progress      chan string
	appendErr     func(error)
	allChunks     hash.HashSet
	chunksByType  map[string][]hash.Hash
	proccessedCnt uint32
}

func newRoundTripper(gs *nbs.GenerationalNBS, progress chan string, appendErr func(error)) (*roundTripper, error) {
	chunkCount, err := gs.OldGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount2, err := gs.NewGen().Count()
	if err != nil {
		return nil, err
	}
	chunkCount += chunkCount2

	vs := types.NewValueStore(gs)

	return &roundTripper{
		vs:           vs,
		gs:           gs,
		chunkCount:   chunkCount,
		progress:     progress,
		appendErr:    appendErr,
		allChunks:    make(hash.HashSet),
		chunksByType: make(map[string][]hash.Hash),
	}, nil
}

func (rt *roundTripper) scanAll(ctx context.Context) error {
	err := rt.gs.OldGen().IterateAllChunks(ctx, rt.roundTripAndCategorizeChunk)
	if err != nil {
		return err
	}
	err = rt.gs.NewGen().IterateAllChunks(ctx, rt.roundTripAndCategorizeChunk)
	if err != nil {
		return err
	}
	return nil
}

func (rt *roundTripper) decodeMsg(chk chunks.Chunk) string {
	hrs := ""
	val, err := types.DecodeValue(chk, rt.vs)
	if err == nil {
		hrs = val.HumanReadableString()
	} else {
		hrs = fmt.Sprintf("Unable to decode value: %s", err.Error())
	}
	return hrs
}

func (rt *roundTripper) roundTripAndCategorizeChunk(chunk chunks.Chunk) {
	chunkOk := true
	rt.proccessedCnt++
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
			hrs := rt.decodeMsg(chunk)
			rt.appendErr(errors.New(fmt.Sprintf("Chunk: %s content hash mismatch: %s\n%s", h.String(), calcChkSum.String(), hrs)))
			chunkOk = false
		}

		h = calcChkSum
	}

	var chunkType string
	if len(raw) >= serial.MessagePrefixSz+4 { // Check if we have enough bytes for a serial message
		fileID := serial.GetFileID(raw)
		if fileID != "" {
			chunkType = fileID
		} else {
			chunkType = "UNKNOWN"
		}
	} else {
		chunkType = "TOO_SHORT"
	}

	rt.chunksByType[chunkType] = append(rt.chunksByType[chunkType], h)

	// Add chunk to our set of all found chunks
	rt.allChunks.Insert(h)

	if chunkOk {
		// Round trip validation. Ensure that the top level store returns the same data.
		c, err := rt.gs.Get(context.TODO(), h)
		if err != nil {
			rt.appendErr(errors.New(fmt.Sprintf("Chunk: %s load failed with error: %s", h.String(), err.Error())))
			chunkOk = false
		} else if bytes.Compare(raw, c.Data()) != 0 {
			hrs := rt.decodeMsg(chunk)
			rt.appendErr(errors.New(fmt.Sprintf("Chunk: %s read with incorrect ID: %s\n%s", h.String(), c.Hash().String(), hrs)))
			chunkOk = false
		}
	}

	percentage := (float64(rt.proccessedCnt) * 100) / float64(rt.chunkCount)
	result := fmt.Sprintf("(%4.1f%% done)", percentage)

	progStr := "OK: " + h.String()
	if !chunkOk {
		progStr = "FAIL: " + h.String()
	}
	progStr = result + " " + progStr
	rt.progress <- progStr
}

// validateTreeAndTrackChunks validates each commit's content and structure (trees, referenced objects)
// but does NOT follow parent hashes (no DAG traversal). Parent hashes are validated but not followed.
func validateTreeAndTrackChunks(
	ctx context.Context,
	vs *types.ValueStore,
	reachableCommits *hash.HashSet,
	progress chan string,
	appendErr func(error),
) (*hash.HashSet, error) {

	reachableChunks := &hash.HashSet{}
	treeScnr := newTreeScanner(vs, reachableChunks, appendErr, progress)

	for commitHash := range *reachableCommits {
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
			err = treeScnr.processCommitContent(ctx, commitHash, serialMsg, reachableCommits)
			if err != nil {
				// Any Errors here are unexpected. appendErr should be used for things we expect to possibly fail.
				return nil, err
			}
		} else {
			// Spit on the old format.
			panic(fmt.Sprintf("hash %s is not a SerialMessage commit, got type %T", commitHash.String(), commitValue))
		}
	}

	return reachableChunks, nil
}

type treeScanner struct {
	vs *types.ValueStore
	// avoid re-validation of chunks we've already seen. If the chunk hash is in this map, we've already validated it.
	// If the value is non-nil, it indicates an error was found during prior validation.
	history         map[hash.Hash]*error
	appendErr       func(error)
	reachableChunks *hash.HashSet
	progress        chan string
}

func newTreeScanner(vs *types.ValueStore, reachableChunks *hash.HashSet, appendErr func(error), progress chan string) *treeScanner {
	if reachableChunks == nil {
		panic("wtf")
	}

	return &treeScanner{
		vs:              vs,
		history:         make(map[hash.Hash]*error),
		appendErr:       appendErr,
		reachableChunks: reachableChunks,
		progress:        progress,
	}
}

// processCommitContent validates a commit's structure and referenced objects
// but does NOT follow parent hashes (no DAG traversal)
func (ts *treeScanner) processCommitContent(
	ctx context.Context,
	commitHash hash.Hash, // Use just for error messages.
	serialMsg types.SerialMessage,
	reachableCommits *hash.HashSet, // Set of all commits found in the second phase. Used to validate parent hashes exist.
) error {
	// Validate the file ID is correct for a commit
	fileID := serial.GetFileID(serialMsg)
	if fileID != serial.CommitFileID {
		return fmt.Errorf("runtime error: commit %s has incorrect file ID: expected %s, got %s", commitHash.String(), serial.CommitFileID, fileID)
	}

	// Parse the SerialMessage as a commit
	var commit serial.Commit
	err := serial.InitCommitRoot(&commit, serialMsg, serial.MessagePrefixSz)
	if err != nil {
		ts.appendErr(fmt.Errorf("::commit: %s: failed to deserialize commit: %w", commitHash.String(), err))
		return nil // Continue processing other commits
	}

	// Get the root tree hash and validate the tree structure
	rootBytes := commit.RootBytes()
	if len(rootBytes) == hash.ByteLen {
		rootHash := hash.New(rootBytes)
		ts.progress <- fmt.Sprintf("Validating commit %s's tree: %s", commitHash.String(), rootHash.String())

		err = ts.validateTreeRoot(ctx, commitHash, rootHash)
		if err != nil {
			// Any Errors here are unexpected. appendErr should be used for things we expect to possibly fail.
			return fmt.Errorf("error validating commit %s: root tree %s: %w", commitHash.String(), rootHash.String(), err)
		}
	} else {
		ts.appendErr(fmt.Errorf("::commit: %s: invalid root tree length: %d", commitHash.String(), len(rootBytes)))
		return nil
	}

	// Validate parent hashes exist in our commit set
	parentAddrs, err := types.SerialCommitParentAddrs(ts.vs.Format(), serialMsg)
	if err != nil {
		ts.appendErr(fmt.Errorf("::commit: %s: failed to get parent addresses: %w", commitHash.String(), err))
		return nil
	}
	for _, parentHash := range parentAddrs {
		if !reachableCommits.Has(parentHash) {
			ts.appendErr(fmt.Errorf("commit %s references missing parent %s", commitHash.String(), parentHash.String()))
		}
	}

	// Get and validate the parent closure (commit closure) reference
	parentClosureBytes := commit.ParentClosureBytes()
	if len(parentClosureBytes) == hash.ByteLen {
		parentClosureHash := hash.New(parentClosureBytes)
		if !parentClosureHash.IsEmpty() {
			ts.reachableChunks.Insert(parentClosureHash) // Mark parent closure as reachable

			// Closure will be a single object
			value, err := ts.vs.ReadValue(ctx, parentClosureHash)
			if err != nil {
				ts.appendErr(fmt.Errorf("Commit %s is missing data. Failed to read commit closure %s: %w", commitHash.String(), parentClosureHash.String(), err))
			} else if value == nil {
				ts.appendErr(fmt.Errorf("Commit %s is missing data. Failed to read commit closure %s", commitHash.String(), parentClosureHash.String()))
			} else {
				// NM4 - TODO: Validate closure contents. We should probably do this after we walk the graph,
				// then confirm all parents were seen.
				// progress <- fmt.Sprintf("Found commit closure: %s", parentClosureHash.String())
			}
		} else if len(parentAddrs) != 0 {
			// Empty closure should happen only for root commits. Make sure there are no parents.
			ts.appendErr(fmt.Errorf("commit %s has empty parent closure but has %d parents", commitHash.String(), len(parentAddrs)))
		}

	} else {
		panic(fmt.Sprintf("invalid parent closure length: %d", len(parentClosureBytes)))
	}

	// NM4 - TODO: Validate signatures. Require public keys. Do we do this anywhere?
	// commit.Signature()

	return nil
}

// validateTreeRoot performs breadth-first validation of a tree structure and tracks all reachable chunks and work peformed
func (ts *treeScanner) validateTreeRoot(
	ctx context.Context,
	commitHash, // Use just for error messages.
	treeHash hash.Hash,
) error {
	// Skip if already processed. Root hashes rarely repeat, but possible if you revert to a previous state.
	if e, ok := ts.history[treeHash]; ok {
		if e != nil {
			// Previously processed this chunk and found an error
			// NM4 - customize error message
			ts.appendErr(*e)
		}
		return nil
	}

	ts.reachableChunks.Insert(treeHash)

	// Load the tree
	treeValue, err := ts.vs.ReadValue(ctx, treeHash)
	if err != nil || treeValue == nil {
		err2 := fmt.Errorf("commit::%s: failed to read tree %s: %w", commitHash.String(), treeHash.String(), err)
		ts.history[treeHash] = &err2
		ts.appendErr(err2)
		return nil
	}

	// Handle SerialMessage trees only
	if _, ok := treeValue.(types.SerialMessage); ok {
		// id := serial.GetFileID(serialMsg)
		err = ts.validateSerialMsgTree(ctx, commitHash, treeHash)
		if err != nil {
			// NM4 - not sure if we should return here or continue. TBD.
			return fmt.Errorf("failed to validate tree object %s: %w", treeHash.String(), err)
		}
	} else {
		// Spit on the old format.
		panic(fmt.Sprintf("hash %s is not a SerialMessage tree, got type %T", treeHash.String(), treeValue))
	}

	return nil
}

// validateSerialTreeAndTrack handles validation of SerialMessages which are trees of objects. We want to track all reachable chunks,
// and any errors encountered during traversal are appended via appendErr. If this function returns an error, it indicates an unexpected failure,
// and further processing should halt.
func (ts *treeScanner) validateSerialMsgTree(
	ctx context.Context,
	commitHash hash.Hash, // Uses for error messages only.
	treeHash hash.Hash,
) error {
	var workQueue []hash.Hash
	workQueue = append(workQueue, treeHash)
	for len(workQueue) > 0 {
		currentChunkHash := workQueue[0]
		workQueue = workQueue[1:]
		value, err := ts.vs.ReadValue(ctx, currentChunkHash)
		if err != nil || value == nil {
			err2 := fmt.Errorf("commit::%s: missing chunk %s", commitHash.String(), currentChunkHash.String())
			if err != nil {
				err2 = fmt.Errorf("commit::%s: failed to read chunk %s: %w", commitHash.String(), currentChunkHash.String(), err)
			}

			ts.history[currentChunkHash] = &err2
			ts.appendErr(err2)
			continue
		}

		if serialMsg, ok := value.(types.SerialMessage); ok {
			err := serialMsg.WalkAddrs(ts.vs.Format(), func(addr hash.Hash) error {
				ts.reachableChunks.Insert(addr)

				if e, ok := ts.history[addr]; ok {
					if e != nil {
						newErr := fmt.Errorf("commit::%s: referenced chunk %s (duplicate error)", commitHash.String(), addr.String())
						ts.appendErr(newErr)
					}
					// Already processed this chunk
					return nil
				}

				// Add to the work queue for further processing
				workQueue = append(workQueue, addr)

				return nil
			})
			if err != nil {
				// We intentionally never return errors from WalkAddrs, so any error here is unexpected. Halt.
				return fmt.Errorf("failed to walk references in tree %s: %w", treeHash.String(), err)
			}

			// We managed to load the current value and walk its references without error. Mark it as successfully processed.
			ts.history[currentChunkHash] = nil

		} else {
			panic(fmt.Sprintf("commit::%s: referenced chunk %s from tree %s is not a SerialMessage, got type %T", commitHash.String(), currentChunkHash.String(), treeHash.String(), value))
		}

		//
	}

	return nil
}

// walkCommitDAGFromRefs loads all branches/tags and walks the commit DAG to find reachable commits
// This is lightweight - only validates commit objects, parent closures, and parent hashes (no trees)
func walkCommitDAGFromRefs(ctx context.Context, gs *nbs.GenerationalNBS, allCommits *hash.HashSet, progress chan string, appendErr func(error)) (hash.HashSet, error) {
	startingCommits, err := getRawReferencesFromStoreRoot(ctx, gs, progress, appendErr)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw references from store root: %w", err)
	}

	refCount := 0
	for _, refs := range startingCommits {
		refCount += len(refs)
	}
	progress <- fmt.Sprintf("Found %d refs pointing to %d unique starting commits", refCount, len(startingCommits))

	if len(startingCommits) == 0 {
		progress <- "No refs found - no commits are reachable"
		return make(hash.HashSet), nil
	}

	// Walk the commit DAG from starting commits
	reachableCommits := make(hash.HashSet)
	var commitQueue []hash.Hash
	for commitHash := range startingCommits {
		commitQueue = append(commitQueue, commitHash)
	}

	vs := types.NewValueStore(gs)

	for len(commitQueue) > 0 {
		commitHash := commitQueue[0]
		commitQueue = commitQueue[1:]

		if reachableCommits.Has(commitHash) {
			continue
		}
		reachableCommits.Insert(commitHash)

		// Skip if this commit doesn't exist in our found commits
		if !allCommits.Has(commitHash) {
			appendErr(fmt.Errorf("ref points to missing commit %s", commitHash.String()))
			continue
		}

		commitValue, err := vs.ReadValue(ctx, commitHash)
		if err != nil {
			appendErr(fmt.Errorf("commit %s is missing or corrupted", commitHash.String()))
			continue
		}

		if serialMsg, ok := commitValue.(types.SerialMessage); ok {
			parentAddrs, err := types.SerialCommitParentAddrs(vs.Format(), serialMsg)
			if err != nil {
				appendErr(fmt.Errorf("commit %s has corrupted parent data", commitHash.String()))
				continue
			}

			// Add parents to queue for processing
			for _, parentHash := range parentAddrs {
				if !reachableCommits.Has(parentHash) {
					commitQueue = append(commitQueue, parentHash)
				}
			}
		} else {
			panic(fmt.Sprintf("commit %s is not a SerialMessage, got type %T", commitHash.String(), commitValue))
		}
	}

	// NM4 - do we want to report which commit objects were unreachable from branches/tags?

	progress <- fmt.Sprintf("Found %d commits reachable from branches/tags", len(reachableCommits))
	return reachableCommits, nil
}

// getRawReferencesFromStoreRoot accesses raw references from the chunk store root.
// Returns a map from commit hash to list of reference names that point to it.
func getRawReferencesFromStoreRoot(ctx context.Context, cs chunks.ChunkStore, progress chan string, appendErr func(error)) (map[hash.Hash][]string, error) {
	// Get the root hash from the chunk store
	rootHash, err := cs.Root(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get store root hash: %w", err)
	}

	if rootHash.IsEmpty() {
		// Is an empty root ever valid? Return empty map. NM4.
		return map[hash.Hash][]string{}, nil
	}

	// Get the store root chunk
	rootChunk, err := cs.Get(ctx, rootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get store root chunk: %w", err)
	}
	if rootChunk.IsEmpty() {
		return nil, fmt.Errorf("store root chunk is empty")
	}

	rootData := rootChunk.Data()

	if serial.GetFileID(rootData) != serial.StoreRootFileID {
		return nil, fmt.Errorf("expected store root file id, got: %s", serial.GetFileID(rootData))
	}

	sr, err := serial.TryGetRootAsStoreRoot(rootData, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
	mapBytes := sr.AddressMapBytes()
	node, fileId, err := tree.NodeFromBytes(mapBytes)
	if err != nil {
		return nil, err
	}
	if fileId != serial.AddressMapFileID {
		return nil, fmt.Errorf("unexpected file ID for address map, expected %s, found %s", serial.AddressMapFileID, fileId)
	}

	ns := tree.NewNodeStore(cs)
	addressMap, err := prolly.NewAddressMap(node, ns)
	if err != nil {
		return nil, err
	}

	// Extract references into a map[hash.Hash][]string, filtering for commit-pointing refs only
	refs := make(map[hash.Hash][]string)
	err = addressMap.IterAll(ctx, func(name string, addr hash.Hash) error {
		// Parse the reference using the ref package to determine its type
		if ref.IsRef(name) {
			doltRef, err := ref.Parse(name)
			if err != nil {
				// NM4 - probably shouldn't skip silently. TBD.
				progress <- fmt.Sprintf("Skipping invalid ref: %s (parse error: %v)", name, err)
				return nil
			}

			refType := doltRef.GetType()
			switch refType {
			case ref.BranchRefType, ref.RemoteRefType, ref.InternalRefType, ref.WorkspaceRefType, ref.StashRefType: // NM4 - not sure about stash type...
				// Address is the commit id.
				refs[addr] = append(refs[addr], name)
			case ref.TagRefType:
				if commitHash, ok := resolveTagToCommit(ctx, cs, name, addr, progress, appendErr); ok {
					refs[commitHash] = append(refs[commitHash], name)
				}
			default:
				// NM4 - probably shouldn't skip silently. TBD.
				progress <- fmt.Sprintf("Skipping ref type %s: %s", refType, name)
			}
		} else if ref.IsWorkingSet(name) {
			// skip.
		} else {
			return fmt.Errorf("invalid ref name found in root address map: %s", name)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to iterate address map: %w", err)
	}

	return refs, nil
}

// resolveTagToCommit reads a tag object and extracts the commit hash it points to
// Returns the commit hash and true if successful, or zero hash and false if there was an error
func resolveTagToCommit(ctx context.Context, cs chunks.ChunkStore, tagName string, tagAddr hash.Hash, progress chan string, appendErr func(error)) (hash.Hash, bool) {
	// Get the tag object from the chunk store
	tagChunk, err := cs.Get(ctx, tagAddr)
	if err != nil {
		appendErr(fmt.Errorf("failed to read tag object %s: %w", tagAddr.String(), err))
		return hash.Hash{}, false
	}

	if tagChunk.IsEmpty() {
		appendErr(fmt.Errorf("tag object %s is empty", tagAddr.String()))
		return hash.Hash{}, false
	}

	// Parse the tag object to get the commit hash it points to
	tagData := tagChunk.Data()
	if serial.GetFileID(tagData) != serial.TagFileID {
		appendErr(fmt.Errorf("tag object %s has incorrect file ID: expected %s, got %s", tagAddr.String(), serial.TagFileID, serial.GetFileID(tagData)))
		return hash.Hash{}, false
	}

	var tag serial.Tag
	err = serial.InitTagRoot(&tag, tagData, serial.MessagePrefixSz)
	if err != nil {
		appendErr(fmt.Errorf("failed to parse tag object %s: %w", tagAddr.String(), err))
		return hash.Hash{}, false
	}

	// Extract the commit hash from the tag
	commitBytes := tag.CommitAddrBytes()
	if len(commitBytes) != hash.ByteLen {
		appendErr(fmt.Errorf("tag %s has invalid commit address length: %d", tagName, len(commitBytes)))
		return hash.Hash{}, false
	}

	commitHash := hash.New(commitBytes)
	return commitHash, true
}
