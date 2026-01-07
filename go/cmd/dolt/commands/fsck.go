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
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

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

	"container/list"
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

// Exec re-loads the database, and verifies the integrity of all chunks (referenced or not), walks the commit DAG, then
// validates all reachable trees and their referenced objects.
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
	progress := make(chan FsckProgressMessage, 32)
	var errs Errs

	params := make(map[string]interface{})
	params[dbfactory.ChunkJournalParam] = struct{}{}
	dbFact := dbfactory.FileFactory{}
	ddb, _, _, err := dbFact.CreateDbNoCache(ctx, types.Format_Default, u, params, func(vErr error) {
		errs.AppendE(vErr)
	})
	if err != nil {
		if errors.Is(err, nbs.ErrJournalDataLoss) {
			cli.PrintErrln("WARNING: Chunk journal is corrupted and some data may be lost.")
			cli.PrintErrln("Run `dolt fsck --revive-journal-with-data-loss` to attempt to recover the journal by")
			cli.PrintErrln("discarding invalid data blocks. Your original data will be preserved in a backup file.")
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

	terminate = func() bool {
		defer close(progress)
		err := fsckOnChunkStore(ctx, gs, &errs, progress)
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

	return errs.PrintAll()
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

// Errs is a slice of errors encountered during fsck processing. It has helper for adding to it and printing it.
type Errs []error

func (e *Errs) AppendF(msg string, args ...any) {
	*e = append(*e, fmt.Errorf(msg, args...))
}

// CmtAppendF appends an error message prefixed with a specific commit hash. The error returned is the error created
// and appended. Not an indication of success or failure, so can be ignored.
func (e *Errs) CmtAppendF(commitHash hash.Hash, msg string, args ...any) error {
	msg = fmt.Sprintf("::commit:%s: %s", commitHash.String(), msg)
	newErr := fmt.Errorf(msg, args...)
	*e = append(*e, newErr)
	return newErr
}
func (e *Errs) AppendE(err error) {
	*e = append(*e, err)
}

func (e *Errs) PrintAll() int {
	if len(*e) == 0 {
		cli.Println("No problems found.")
		return 0
	} else {
		for _, err := range *e {
			cli.Println(color.RedString("------ Corruption Found ------"))
			cli.Println(err.Error())
		}

		return 1
	}
}

// ProgressReporter is a channel for reporting progress messages during fsck. There is a dedicated goroutine that
// pulls off messages and displays them to the user. FSCK can be a long process, so progress reporting is important.
type ProgressReporter chan FsckProgressMessage

func (pr ProgressReporter) Milestonef(ctx context.Context, msg string, args ...any) {
	pr.Insert(ctx, FsckProgressMessage{Type: FsckProgressMilestone, Message: fmt.Sprintf(msg, args...)})
}

func (pr ProgressReporter) Milestone(ctx context.Context, msg string) {
	pr.Insert(ctx, FsckProgressMessage{Type: FsckProgressMilestone, Message: msg})
}

func (pr ProgressReporter) Insert(ctx context.Context, msg FsckProgressMessage) {
	select {
	case <-ctx.Done():
		return
	case pr <- msg:
	}
}

// FsckProgressMessageType indicates the type of progress message
type FsckProgressMessageType int

const (
	// Milestone messages that should always be displayed
	FsckProgressMilestone FsckProgressMessageType = iota
	// Ephemeral chunk scanning progress with percentage
	FsckProgressChunkScan
	// Ephemeral tree validation progress
	FsckProgressTreeValidation
)

// FsckProgressMessage represents a structured progress update during fsck
type FsckProgressMessage struct {
	Type       FsckProgressMessageType
	Message    string
	Percentage float64 // Optional percentage for progress tracking
	Current    int     // Optional current item count
	Total      int     // Optional total item count
}

// fsckHandleProgress processes progress messages from the fsck operation and displays them to the user.
func fsckHandleProgress(ctx context.Context, progress ProgressReporter, quiet bool) {
	if quiet {
		// Just drain the progress channel without displaying anything
		for range progress {
			if ctx.Err() != nil {
				return
			}
		}
		return
	}

	var spinney Spinner
	p := cli.NewEphemeralPrinter()
	lastUpdateTime := time.Now()
	var currentEphemeralMsg *FsckProgressMessage

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-progress:
			if !ok {
				return
			}

			switch msg.Type {
			case FsckProgressMilestone:
				// Clear any existing ephemeral message and print the persistent message
				if currentEphemeralMsg != nil {
					p.Printf("\n")
					p.Display()
					currentEphemeralMsg = nil
				}
				cli.Println(msg.Message)

			case FsckProgressChunkScan, FsckProgressTreeValidation:
				// Store ephemeral message for in-line updates
				currentEphemeralMsg = &msg
			}

		// Update ephemeral display every second
		case <-time.After(1 * time.Second):
		}

		// Update spinner and ephemeral message
		if currentEphemeralMsg != nil && time.Since(lastUpdateTime) > 1*time.Second {
			spinney.Tick()
			spinChr := spinney.Text()

			// Display with percentage and/or count information if available
			var operation string
			switch currentEphemeralMsg.Type {
			case FsckProgressChunkScan:
				operation = "Scanning chunks"
			case FsckProgressTreeValidation:
				operation = "Validating trees"
			default:
				operation = ""
			}

			if operation != "" && currentEphemeralMsg.Percentage > 0.0 && currentEphemeralMsg.Total > 0 {
				p.Printf("%s %s: %d/%d (%.1f%% complete)", spinChr, operation, currentEphemeralMsg.Current, currentEphemeralMsg.Total, currentEphemeralMsg.Percentage)
			} else if operation != "" && currentEphemeralMsg.Percentage > 0.0 {
				p.Printf("%s %s: %.1f%% complete", spinChr, operation, currentEphemeralMsg.Percentage)
			} else {
				p.Printf("%s %s", spinChr, currentEphemeralMsg.Message)
			}
			p.Display()
			lastUpdateTime = time.Now()
		}
	}
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
// As with the other code in this file, we try and continue processing as much as possible even in the presence of corruption,
// so that a full report can be generated. Errors encountered during processing are appended to the |errs| object. Only
// when there is an unexpected failure (such as inability to open a storage file) is an error returned. In that situation,
// we halt processing.
func fsckOnChunkStore(ctx context.Context, gs *nbs.GenerationalNBS, errs *Errs, progress ProgressReporter) error {
	rt, err := newRoundTripper(ctx, gs, progress, errs)
	if err != nil {
		return fmt.Errorf("failed to initialize FSCK round tripper: %w", err)
	}
	err = rt.scanAll(ctx)
	if err != nil {
		return fmt.Errorf("failed during full chunk scan: %w", err)
	}

	chunkCount := rt.chunkCount
	chunksByType := rt.chunksByType

	// Report chunk type summary
	progress.Milestone(ctx, "--------------- Chunk Type Summary ---------------")
	for chunkType, hashes := range chunksByType {
		progress.Milestonef(ctx, "Found %d chunks of type: %s", len(hashes), chunkType)
	}

	// Perform commit DAG validation from all branch HEADs and tags to identify unreachable chunks
	progress.Milestone(ctx, "--------------- All Objects scanned. Starting commit validation ---------------")

	// Find all commit objects from our scanned chunks
	allCommitsSet := make(hash.HashSet)
	if commitChunks, hasCommits := chunksByType[serial.CommitFileID]; hasCommits {
		for _, commitHash := range commitChunks {
			allCommitsSet.Insert(commitHash)
		}
		progress.Milestonef(ctx, "Found %d commit objects", len(allCommitsSet))
	} else {
		progress.Milestone(ctx, "No commit objects found during chunk scan")
	}

	reachableCommits, err := walkCommitDAGFromRefs(ctx, gs, &allCommitsSet, progress, errs)
	if err != nil {
		return fmt.Errorf("commit DAG walking failed: %w", err)
	}

	// Phase 3: Tree validation for commits (performance heavy)
	if len(reachableCommits) > 0 {
		progress.Milestonef(ctx, "Starting tree validation for %d commit objects...", len(reachableCommits))

		vs := types.NewValueStore(gs)

		commitReachableChunks, err := validateCommitTrees(ctx, vs, gs, &reachableCommits, progress, errs)
		if err != nil {
			return fmt.Errorf("commit tree validation failed: %w", err)
		}

		// Report which commits are reachable vs unreachable from branches/tags.
		unreachableCommits := 0
		for commitHash := range allCommitsSet {
			if !reachableCommits.Has(commitHash) {
				unreachableCommits++
			}
		}
		unreachableChunks := chunkCount - uint32(commitReachableChunks.Size())

		progress.Milestonef(ctx, "Found %d unreachable commits (not reachable from any branch/tag)", unreachableCommits)
		progress.Milestonef(ctx, "Validated %d chunks reachable by branches and tags (unreachable: %d)", commitReachableChunks.Size(), unreachableChunks)
	} else {
		progress.Milestone(ctx, "No branches or tags found. Skipping tree validation.")
	}

	return nil
}

// roundTripper performs a full scan of all chunks, verifying that their hashes match their content.
type roundTripper struct {
	ctx           context.Context
	vs            *types.ValueStore
	gs            *nbs.GenerationalNBS
	chunkCount    uint32
	progress      ProgressReporter
	errs          *Errs
	allChunks     hash.HashSet
	chunksByType  map[string][]hash.Hash
	proccessedCnt uint32
}

func newRoundTripper(ctx context.Context, gs *nbs.GenerationalNBS, progress chan FsckProgressMessage, errs *Errs) (*roundTripper, error) {
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
		ctx:          ctx,
		vs:           vs,
		gs:           gs,
		chunkCount:   chunkCount,
		progress:     progress,
		errs:         errs,
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

// roundTripAndCategorizeChunk verifies the chunk's hash matches its content, categorizes it by type. This method is
// passed as an argument to the chunk store's IterateAllChunks method, so interface mustn't change.
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
			rt.errs.AppendF("Chunk: %s content hash mismatch: %s\n%s", h.String(), calcChkSum.String(), hrs)
			chunkOk = false
		}

		// Use the calculated checksum going forward. This ensures that the categorizations and round trip loads use the correct hash.
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
		c, err := rt.gs.Get(rt.ctx, h)
		if err != nil {
			rt.errs.AppendF("Chunk: %s load failed with error: %w", h.String(), err)
			chunkOk = false
		} else if bytes.Compare(raw, c.Data()) != 0 {
			hrs := rt.decodeMsg(chunk)
			rt.errs.AppendF("Chunk: %s read with incorrect ID: %s\n%s", h.String(), c.Hash().String(), hrs)
			chunkOk = false
		}
	}

	percentage := (float64(rt.proccessedCnt) * 100) / float64(rt.chunkCount)

	status := "OK"
	if !chunkOk {
		status = "FAIL"
	}

	rt.progress.Insert(rt.ctx, FsckProgressMessage{
		Type:       FsckProgressChunkScan,
		Message:    fmt.Sprintf("%s: %s", status, h.String()),
		Percentage: percentage,
		Current:    int(rt.proccessedCnt),
		Total:      int(rt.chunkCount),
	})
}

// decodeMsg attempts to decode the chunk into a human-readable string for error reporting.
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

// validateCommitTrees validates each commit's content and structure (trees, referenced objects)
// but does not follow parent hashes (no DAG traversal). Parent hashes are validated but not followed.
func validateCommitTrees(
	ctx context.Context,
	vs *types.ValueStore,
	cs chunks.ChunkStore,
	reachableCommits *hash.HashSet,
	progress ProgressReporter,
	errs *Errs,
) (*hash.HashSet, error) {

	reachableChunks := &hash.HashSet{}
	ns := tree.NewNodeStore(cs)
	treeScnr := newTreeScanner(vs, ns, reachableChunks, errs, progress)

	totalCommits := len(*reachableCommits)
	processedCommits := 0

	for commitHash := range *reachableCommits {
		processedCommits++
		percentage := (float64(processedCommits) * 100) / float64(totalCommits)

		// Send progress update for tree validation
		progress.Insert(ctx, FsckProgressMessage{
			Type:       FsckProgressTreeValidation,
			Message:    fmt.Sprintf("Validating commit %s", commitHash.String()),
			Percentage: percentage,
			Current:    processedCommits,
			Total:      totalCommits,
		})
		// Load and validate the commit
		commitValue, err := vs.MustReadValue(ctx, commitHash)
		if err != nil {
			_ = errs.CmtAppendF(commitHash, "read failure: %w", err)
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
			panic(fmt.Sprintf("Commit %s is not a SerialMessage commit, got type %T", commitHash.String(), commitValue))
		}
	}

	return reachableChunks, nil
}

// treeScanner walks and validates prolly tree structures.
type treeScanner struct {
	vs *types.ValueStore
	ns tree.NodeStore
	// avoid re-validation of chunks we've already seen. If the chunk hash is in this map, we've already validated it.
	// If the value is non-nil, it indicates an error was found during prior validation.
	history         map[hash.Hash]*error
	errs            *Errs
	reachableChunks *hash.HashSet
	progress        chan FsckProgressMessage
}

func newTreeScanner(vs *types.ValueStore, ns tree.NodeStore, reachableChunks *hash.HashSet, errs *Errs, progress chan FsckProgressMessage) *treeScanner {
	return &treeScanner{
		vs:              vs,
		ns:              ns,
		history:         make(map[hash.Hash]*error),
		errs:            errs,
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
		// This shouldn't happen since we got all commits from the chunk type categorization phase.
		return fmt.Errorf("runtime error: commit %s has incorrect file ID: expected %s, got %s", commitHash.String(), serial.CommitFileID, fileID)
	}

	// Parse the SerialMessage as a commit
	var commit serial.Commit
	err := serial.InitCommitRoot(&commit, serialMsg, serial.MessagePrefixSz)
	if err != nil {
		_ = ts.errs.CmtAppendF(commitHash, "failed to deserialize commit: %w", err)
		return nil // Continue processing other commits
	}

	// Get the root tree hash and validate the tree structure
	rootBytes := commit.RootBytes()
	if len(rootBytes) == hash.ByteLen {
		rootHash := hash.New(rootBytes)
		err = ts.validateTreeRoot(ctx, commitHash, rootHash)
		if err != nil {
			// Any Errors here are unexpected. appendErr should be used for things we expect to possibly fail.
			return fmt.Errorf("error validating commit %s: root tree %s: %w", commitHash.String(), rootHash.String(), err)
		}
	} else {
		_ = ts.errs.CmtAppendF(commitHash, "invalid root tree length: %d", len(rootBytes))
		return nil
	}

	// Validate parent hashes exist in our commit set
	parentAddrs, err := types.SerialCommitParentAddrs(ts.vs.Format(), serialMsg)
	if err != nil {
		_ = ts.errs.CmtAppendF(commitHash, "failed to get parent addresses: %w", err)
		return nil
	}
	for _, parentHash := range parentAddrs {
		if !reachableCommits.Has(parentHash) {
			_ = ts.errs.CmtAppendF(commitHash, "references missing parent %s", parentHash.String())
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
				_ = ts.errs.CmtAppendF(commitHash, "missing data. failed to read commit closure %s: %w", parentClosureHash.String(), err)
			} else if value == nil {
				_ = ts.errs.CmtAppendF(commitHash, "missing data. failed to read commit closure %s", parentClosureHash.String())
			} else {
				// All hashes in the closure should be reachable commits.
				// Use the proper commit closure approach instead of WalkAddrs
				closure, err := datas.NewParentsClosure(ctx, nil, serialMsg, ts.vs, ts.ns)
				if err != nil {
					_ = ts.errs.CmtAppendF(commitHash, "failed to load parent closure %s: %w", parentClosureHash.String(), err)
				} else if !closure.IsEmpty() {
					// Verify all commits in the closure are things we expect to see.
					iter, err := closure.IterAllReverse(ctx)
					if err != nil {
						_ = ts.errs.CmtAppendF(commitHash, "failed to get iterator for parent closure %s: %w", parentClosureHash.String(), err)
					} else {
						for {
							key, _, err := iter.Next(ctx)
							if err == io.EOF {
								break
							}
							if err != nil {
								_ = ts.errs.CmtAppendF(commitHash, "failure iterating parent closure %s: %w", parentClosureHash.String(), err)
								break
							}

							closureCommitAddr := key.Addr()
							if !reachableCommits.Has(closureCommitAddr) {
								_ = ts.errs.CmtAppendF(commitHash, "parent closure references unreachable commit %s", closureCommitAddr.String())
							}
						}
					}
				}
			}
		} else if len(parentAddrs) != 0 {
			// Empty closure should happen only for root commits. Make sure there are no parents.
			_ = ts.errs.CmtAppendF(commitHash, "has empty parent closure but has %d parents", len(parentAddrs))
		}

	} else {
		panic(fmt.Sprintf("invalid parent closure length: %d", len(parentClosureBytes)))
	}

	// TODO: Validate commit signatures.
	// commit.Signature()

	return nil
}

// validateTreeRoot performs breadth-first validation of a tree structure and tracks all reachable chunks in |ts.reachableChunks|
func (ts *treeScanner) validateTreeRoot(
	ctx context.Context,
	commitHash, // Use just for error messages.
	treeHash hash.Hash,
) error {
	// Skip if already processed. Root hashes rarely repeat, but possible if you revert to a previous state.
	if e, ok := ts.history[treeHash]; ok {
		if e != nil {
			ts.errs.CmtAppendF(commitHash, "referenced chunk %s (duplicate error)", treeHash.String())
		}
		return nil
	}

	treeValue, err := ts.vs.ReadValue(ctx, treeHash)
	if err != nil || treeValue == nil {
		err2 := ts.errs.CmtAppendF(commitHash, "failed to read tree %s: %w", treeHash.String(), err)
		ts.history[treeHash] = &err2
		return nil
	}

	if _, ok := treeValue.(types.SerialMessage); ok {
		err = ts.validateTree(ctx, commitHash, treeHash)
		if err != nil {
			return fmt.Errorf("failed to validate tree object %s: %w", treeHash.String(), err)
		}
	} else {
		// Spit on the old format.
		panic(fmt.Sprintf("hash %s is not a SerialMessage tree, got type %T", treeHash.String(), treeValue))
	}

	return nil
}

// validateTree validates SerialMessages which are trees of objects. We track all reachable chunks, and any
// errors encountered during traversal are appended via appendErr. If this function returns an error, it indicates
// an unexpected failure, and further processing should halt.
func (ts *treeScanner) validateTree(
	ctx context.Context,
	commitHash hash.Hash, // Uses for error messages only.
	treeHash hash.Hash,
) error {
	workQueue := list.New()
	workQueue.PushBack(treeHash)

	for workQueue.Len() > 0 {
		elem := workQueue.Front()
		currentChunkHash := elem.Value.(hash.Hash)
		workQueue.Remove(elem)

		ts.reachableChunks.Insert(currentChunkHash)

		value, err := ts.vs.MustReadValue(ctx, currentChunkHash)
		if err != nil {
			err2 := ts.errs.CmtAppendF(commitHash, "read failure of %s: %w", currentChunkHash.String(), err)
			ts.history[currentChunkHash] = &err2
			continue
		}

		if serialMsg, ok := value.(types.SerialMessage); ok {
			err := serialMsg.WalkAddrs(ts.vs.Format(), func(addr hash.Hash) error {
				if e, ok := ts.history[addr]; ok {
					if e != nil {
						_ = ts.errs.CmtAppendF(commitHash, "referenced chunk %s (duplicate error)", addr.String())
					}
					// Already processed this chunk
					return nil
				}

				workQueue.PushBack(addr)

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
	}

	return nil
}

// walkCommitDAGFromRefs loads all branches/tags and walks the commit DAG to find reachable commits
// This is lightweight - only validates commit objects, parent closures, and parent hashes (no trees)
func walkCommitDAGFromRefs(ctx context.Context, gs *nbs.GenerationalNBS, allCommits *hash.HashSet, progress ProgressReporter, errs *Errs) (hash.HashSet, error) {
	startingCommits, err := getRawReferencesFromStoreRoot(ctx, gs, errs)
	if err != nil {
		return nil, fmt.Errorf("failed to get references from store root: %w", err)
	}

	refCount := 0
	for _, refs := range startingCommits {
		refCount += len(refs)
	}
	progress.Milestonef(ctx, "Found %d refs pointing to %d unique starting commits", refCount, len(startingCommits))

	if len(startingCommits) == 0 {
		progress.Milestone(ctx, "No refs found - no commits are reachable")
		return hash.HashSet{}, nil
	}

	// commitQueue is used as the work queue, and reachableCommits tracks all commits we've put in the queue (to avoid double enqueueing)
	commitQueue := list.New()
	reachableCommits := hash.HashSet{}
	for commitHash := range startingCommits {
		commitQueue.PushBack(commitHash)
		reachableCommits.Insert(commitHash)
	}

	vs := types.NewValueStore(gs)

	for commitQueue.Len() > 0 {
		elem := commitQueue.Front()
		commitHash := elem.Value.(hash.Hash)
		commitQueue.Remove(elem)

		// Skip if this commit doesn't exist in our found commits
		if !allCommits.Has(commitHash) {
			_ = errs.CmtAppendF(commitHash, "missing commit object")
			continue
		}

		commitValue, err := vs.ReadValue(ctx, commitHash)
		if err != nil {
			_ = errs.CmtAppendF(commitHash, "read error: %w", err)
			continue
		}

		if serialMsg, ok := commitValue.(types.SerialMessage); ok {
			parentAddrs, err := types.SerialCommitParentAddrs(vs.Format(), serialMsg)
			if err != nil {
				_ = errs.CmtAppendF(commitHash, "corrupted parent data: %w", err)
				continue
			}

			for _, parentHash := range parentAddrs {
				if !reachableCommits.Has(parentHash) {
					commitQueue.PushBack(parentHash)
					reachableCommits.Insert(parentHash)
				}
			}
		} else {
			panic(fmt.Sprintf("::commit:%s: is not a SerialMessage, got type %T", commitHash.String(), commitValue))
		}
	}
	progress.Milestonef(ctx, "Found %d commits reachable from branches/tags", len(reachableCommits))

	return reachableCommits, nil
}

// getRawReferencesFromStoreRoot accesses raw references from the chunk store root.
// Returns a map from commit hash to list of reference names that point to it.
func getRawReferencesFromStoreRoot(ctx context.Context, cs chunks.ChunkStore, errs *Errs) (map[hash.Hash][]string, error) {
	// Get the root hash from the chunk store
	rootHash, err := cs.Root(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get store root hash: %w", err)
	}
	if rootHash.IsEmpty() {
		// Empty root? There should always be something.
		return nil, fmt.Errorf("store root hash is empty")
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
		return nil, fmt.Errorf("invalid root chunk: %s. expected store root file id, got: %s", rootHash.String(), serial.GetFileID(rootData))
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
				return fmt.Errorf("failed to parse ref name %s: %w", name, err)
			}

			refType := doltRef.GetType()
			switch refType {
			case ref.BranchRefType, ref.RemoteRefType, ref.InternalRefType, ref.WorkspaceRefType, ref.StashRefType:
				// Address is the commit id.
				refs[addr] = append(refs[addr], name)
			case ref.TagRefType:
				if commitHash, ok := resolveTagToCommit(ctx, cs, name, addr, errs); ok {
					refs[commitHash] = append(refs[commitHash], name)
				}
			default:
				return fmt.Errorf("unexpected ref type (%s) from ref: %s", refType, name)
			}
		} else if ref.IsWorkingSet(name) {
			// skip.
		} else {
			return fmt.Errorf("invalid ref name (%s)", name)
		}
		return nil
	})

	if err != nil {
		// Failure to iterate address map is unexpected, but possibly recoverable. We'll return the error and give up now,
		// but there may be a future need to continue processing other refs.
		return nil, fmt.Errorf("failed to iterate root address map %s: %w", rootHash.String(), err)
	}

	return refs, nil
}

// resolveTagToCommit reads a tag object and extracts the commit hash it points to
// Returns the commit hash and true if successful, or zero hash and false if there was an error
func resolveTagToCommit(ctx context.Context, cs chunks.ChunkStore, tagName string, tagAddr hash.Hash, errs *Errs) (hash.Hash, bool) {
	// Get the tag object from the chunk store
	tagChunk, err := cs.Get(ctx, tagAddr)
	if err != nil {
		errs.AppendF("failed to read tag object %s: %w", tagAddr.String(), err)
		return hash.Hash{}, false
	}
	if tagChunk.IsEmpty() {
		errs.AppendF("tag object %s is empty", tagAddr.String())
		return hash.Hash{}, false
	}

	// Parse the tag object to get the commit hash it points to
	tagData := tagChunk.Data()
	if serial.GetFileID(tagData) != serial.TagFileID {
		errs.AppendF("tag object %s has incorrect file ID: expected %s, got %s", tagAddr.String(), serial.TagFileID, serial.GetFileID(tagData))
		return hash.Hash{}, false
	}

	var tag serial.Tag
	err = serial.InitTagRoot(&tag, tagData, serial.MessagePrefixSz)
	if err != nil {
		errs.AppendF("failed to parse tag object %s: %w", tagAddr.String(), err)
		return hash.Hash{}, false
	}

	// Extract the commit hash from the tag
	commitBytes := tag.CommitAddrBytes()
	if len(commitBytes) != hash.ByteLen {
		errs.AppendF("tag %s has invalid commit address length: %d", tagName, len(commitBytes))
		return hash.Hash{}, false
	}

	commitHash := hash.New(commitBytes)
	return commitHash, true
}
