// Copyright 2025 Dolthub, Inc.
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

package admin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// Use the exported ChunkSource interface from nbs package

type DebugCompareCmd struct {
}

func (d DebugCompareCmd) Name() string {
	return "debug-compare"
}

func (d DebugCompareCmd) Description() string {
	return "compare contents of a tablefile hash and archive file hash for debugging"
}

func (d DebugCompareCmd) Hidden() bool {
	return true
}

func (d DebugCompareCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	if len(args) != 2 {
		cli.PrintErrln("Usage: debug-compare <tablefile-hash> <archive-hash>")
		return 1
	}

	tablefileHashStr := args[0]
	archiveHashStr := args[1]

	// Parse the hash arguments
	tablefileHash, ok := hash.MaybeParse(tablefileHashStr)
	if !ok {
		cli.PrintErrln(fmt.Sprintf("Invalid tablefile hash: %s", tablefileHashStr))
		return 1
	}

	archiveHash, ok := hash.MaybeParse(archiveHashStr)
	if !ok {
		cli.PrintErrln(fmt.Sprintf("Invalid archive hash: %s", archiveHashStr))
		return 1
	}

	// Get the current directory for storage files
	abs, err := dEnv.FS.Abs("")
	if err != nil {
		cli.PrintErrln(fmt.Sprintf("Couldn't get absolute path: %v", err))
		return 1
	}

	// Try oldgen directory first, then fall back to noms directory
	nomsBaseDir := filepath.Join(abs, ".dolt", "noms")
	nomsOldgenDir := filepath.Join(nomsBaseDir, "oldgen")

	// Check if either noms directory exists
	var searchDir string
	if _, err := os.Stat(nomsOldgenDir); err == nil {
		searchDir = nomsOldgenDir
	} else if _, err := os.Stat(nomsBaseDir); err == nil {
		searchDir = nomsOldgenDir // Pass oldgen dir so the function can check both oldgen and parent
	} else {
		cli.PrintErrln(fmt.Sprintf("NBS directory not found: neither %s nor %s exist", nomsBaseDir, nomsOldgenDir))
		return 1
	}

	cli.Println(fmt.Sprintf("Comparing tablefile %s with archive %s", tablefileHashStr, archiveHashStr))
	cli.Println(fmt.Sprintf("Searching in: %s and %s", nomsOldgenDir, nomsBaseDir))

	// Try to compare the files directly using IterChunks
	cli.Println("\n--- Comparing Contents ---")
	err = compareTableFiles(ctx, searchDir, tablefileHash, archiveHash, tablefileHashStr, archiveHashStr)
	if err != nil {
		cli.PrintErrln(fmt.Sprintf("Comparison failed: %v", err))
		return 1
	}

	cli.Println("Comparison completed successfully!")
	return 0
}

func compareTableFiles(ctx context.Context, dir string, hash1, hash2 hash.Hash, name1, name2 string) error {
	cli.Println(fmt.Sprintf("Comparing %s with %s", name1, name2))

	// Load the first file as a chunkSource
	cs1, fileType1, chunkCount1, err := loadChunkSource(ctx, dir, hash1)
	if err != nil {
		return fmt.Errorf("failed to load %s: %v", name1, err)
	}
	defer closeChunkSource(cs1)
	cli.Println(fmt.Sprintf("Loaded %s file for %s with %d chunks", fileType1, name1, chunkCount1))

	// Load the second file as a chunkSource
	cs2, fileType2, chunkCount2, err := loadChunkSource(ctx, dir, hash2)
	if err != nil {
		return fmt.Errorf("failed to load %s: %v", name2, err)
	}
	defer closeChunkSource(cs2)
	cli.Println(fmt.Sprintf("Loaded %s file for %s with %d chunks", fileType2, name2, chunkCount2))

	// Compare chunk counts
	actualCount1, err := getChunkCount(cs1)
	if err != nil {
		return fmt.Errorf("failed to get count from %s: %v", name1, err)
	}
	actualCount2, err := getChunkCount(cs2)
	if err != nil {
		return fmt.Errorf("failed to get count from %s: %v", name2, err)
	}

	cli.Println(fmt.Sprintf("\nChunk Count Comparison:"))
	cli.Println(fmt.Sprintf("  %s (%s): expected %d, actual %d chunks", name1, fileType1, chunkCount1, actualCount1))
	cli.Println(fmt.Sprintf("  %s (%s): expected %d, actual %d chunks", name2, fileType2, chunkCount2, actualCount2))

	if actualCount1 != actualCount2 {
		cli.Println(fmt.Sprintf("WARNING: Actual chunk counts differ! %s has %d, %s has %d", name1, actualCount1, name2, actualCount2))
	} else {
		cli.Println(fmt.Sprintf("Chunk counts match: %d chunks each", actualCount1))
	}

	// Now we can iterate through the chunks to compare content
	cli.Println(fmt.Sprintf("\nIterating through chunks for detailed comparison..."))

	// Collect hashes from both chunk sources
	hashes1 := make(map[hash.Hash]bool)
	hashes2 := make(map[hash.Hash]bool)

	// Collect from first source
	var count1 uint32
	err = iterateAllChunksFast(ctx, cs1, func(h hash.Hash, chunk chunks.Chunk) error {
		hashes1[h] = true
		count1++
		if count1%10000 == 0 {
			percentage := float64(count1) / float64(actualCount1) * 100
			fmt.Printf("\033[2K\rProcessing %s: %d/%d chunks (%.1f%%)", name1, count1, actualCount1, percentage)
		}
		return nil
	})
	fmt.Printf("\033[2K\rProcessing %s: %d/%d chunks (100.0%%) - Complete\n", name1, count1, actualCount1)
	if err != nil {
		return fmt.Errorf("failed to iterate chunks from %s: %v", name1, err)
	}

	// Collect from second source
	var count2 uint32
	err = iterateAllChunksFast(ctx, cs2, func(h hash.Hash, chunk chunks.Chunk) error {
		hashes2[h] = true
		count2++
		if count2%10000 == 0 {
			percentage := float64(count2) / float64(actualCount2) * 100
			fmt.Printf("\033[2K\rProcessing %s: %d/%d chunks (%.1f%%)", name2, count2, actualCount2, percentage)
		}
		return nil
	})
	fmt.Printf("\033[2K\rProcessing %s: %d/%d chunks (100.0%%) - Complete\n", name2, count2, actualCount2)
	if err != nil {
		return fmt.Errorf("failed to iterate chunks from %s: %v", name2, err)
	}

	// Verify that the number of collected hashes matches the expected chunk counts
	if len(hashes1) != int(count1) {
		cli.Println(fmt.Sprintf("WARNING: %s has duplicate chunks! Iterated %d chunks but collected %d unique hashes", name1, count1, len(hashes1)))
	}
	if len(hashes2) != int(count2) {
		cli.Println(fmt.Sprintf("WARNING: %s has duplicate chunks! Iterated %d chunks but collected %d unique hashes", name2, count2, len(hashes2)))
	}
	if len(hashes1) != int(actualCount1) {
		cli.Println(fmt.Sprintf("WARNING: %s iteration collected %d unique hashes but count() returned %d", name1, len(hashes1), actualCount1))
	}
	if len(hashes2) != int(actualCount2) {
		cli.Println(fmt.Sprintf("WARNING: %s iteration collected %d unique hashes but count() returned %d", name2, len(hashes2), actualCount2))
	}

	// Compare the collected hashes
	return compareChunkHashes(hashes1, hashes2, name1, name2)
}

func loadChunkSource(ctx context.Context, baseDir string, h hash.Hash) (nbs.ChunkSource, string, uint32, error) {
	// Try both possible locations: .dolt/noms and .dolt/noms/oldgen
	searchDirs := []string{
		baseDir,               // .dolt/noms/oldgen (passed in)
		filepath.Dir(baseDir), // .dolt/noms (parent of oldgen)
	}

	for _, dir := range searchDirs {
		// Check for regular table file first
		tablePath := filepath.Join(dir, h.String())
		if _, err := os.Stat(tablePath); err == nil {
			// Get chunk count from table footer
			file, err := os.Open(tablePath)
			if err != nil {
				return nil, "", 0, fmt.Errorf("failed to open table file %s: %v", tablePath, err)
			}

			chunkCount, _, err := nbs.ReadTableFooter(file)
			file.Close()
			if err != nil {
				return nil, "", 0, fmt.Errorf("failed to read table footer from %s: %v", tablePath, err)
			}

			// Create chunk source using NomsFileTableReader
			quotaProvider := &nbs.UnlimitedQuotaProvider{}
			cs, err := nbs.NomsFileTableReader(ctx, tablePath, h, chunkCount, quotaProvider)
			if err != nil {
				return nil, "", 0, fmt.Errorf("failed to create table reader for %s: %v", tablePath, err)
			}

			return cs, "table", chunkCount, nil
		}

		// Check for archive file
		archivePath := filepath.Join(dir, h.String()+".darc")
		if _, err := os.Stat(archivePath); err == nil {
			// Create chunk source using NewArchiveChunkSource (ignores chunkCount and quota)
			quotaProvider := &nbs.UnlimitedQuotaProvider{}
			stats := &nbs.Stats{}
			cs, err := nbs.NewArchiveChunkSource(ctx, dir, h, 0, quotaProvider, stats)
			if err != nil {
				return nil, "", 0, fmt.Errorf("failed to create archive reader for %s: %v", archivePath, err)
			}

			// Get actual chunk count from the archive
			chunkCount, err := getChunkCount(cs)
			if err != nil {
				return nil, "", 0, fmt.Errorf("failed to get count from archive: %v", err)
			}

			return cs, "archive", chunkCount, nil
		}
	}

	return nil, "", 0, fmt.Errorf("file not found for hash %s in any of: %v", h.String(), searchDirs)
}

func compareChunkHashes(hashes1, hashes2 map[hash.Hash]bool, name1, name2 string) error {
	// Find differences
	onlyIn1 := make([]hash.Hash, 0)
	onlyIn2 := make([]hash.Hash, 0)
	common := make([]hash.Hash, 0)

	for h := range hashes1 {
		if hashes2[h] {
			common = append(common, h)
		} else {
			onlyIn1 = append(onlyIn1, h)
		}
	}

	for h := range hashes2 {
		if !hashes1[h] {
			onlyIn2 = append(onlyIn2, h)
		}
	}

	cli.Println(fmt.Sprintf("\nHash Comparison Results:"))
	cli.Println(fmt.Sprintf("  Common chunks: %d", len(common)))
	cli.Println(fmt.Sprintf("  Only in %s: %d", name1, len(onlyIn1)))
	cli.Println(fmt.Sprintf("  Only in %s: %d", name2, len(onlyIn2)))

	// Show some examples of differences if they exist
	if len(onlyIn1) > 0 {
		cli.Println(fmt.Sprintf("\nSample chunks only in %s:", name1))
		for i, h := range onlyIn1 {
			if i >= 5 { // Show only first 5
				break
			}
			cli.Println(fmt.Sprintf("  %s", h.String()))
		}
		if len(onlyIn1) > 5 {
			cli.Println(fmt.Sprintf("  ... and %d more", len(onlyIn1)-5))
		}
	}

	if len(onlyIn2) > 0 {
		cli.Println(fmt.Sprintf("\nSample chunks only in %s:", name2))
		for i, h := range onlyIn2 {
			if i >= 5 { // Show only first 5
				break
			}
			cli.Println(fmt.Sprintf("  %s", h.String()))
		}
		if len(onlyIn2) > 5 {
			cli.Println(fmt.Sprintf("  ... and %d more", len(onlyIn2)-5))
		}
	}

	if len(onlyIn1) == 0 && len(onlyIn2) == 0 {
		cli.Println("✓ All chunks match between the two files!")
	}

	return nil
}

func (d DebugCompareCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		CommandStr: "debug-compare",
		ShortDesc:  "compare contents of a tablefile hash and archive file hash for debugging",
		LongDesc: `Admin command to compare the contents of a tablefile and archive file by their hashes.
This is a temporary debugging command to help analyze differences between table files and archive files.

Both arguments should be hash strings (without file extensions). The command will automatically
detect whether each hash corresponds to a regular table file or an archive file (.darc).`,
		Synopsis: []string{
			"debug-compare <tablefile-hash> <archive-hash>",
		},
		ArgParser: d.ArgParser(),
	}
}

func (d DebugCompareCmd) ArgParser() *argparser.ArgParser {
	return argparser.NewArgParserWithVariableArgs(d.Name())
}

var _ cli.Command = DebugCompareCmd{}

// Helper functions to access internal methods of ChunkSource
func closeChunkSource(cs nbs.ChunkSource) error {
	// First try reflection to call exported Close method
	v := reflect.ValueOf(cs)
	method := v.MethodByName("Close")

	if method.IsValid() {
		// Call the method
		results := method.Call([]reflect.Value{})

		// The Close method should return error
		if len(results) == 1 {
			if !results[0].IsNil() {
				err := results[0].Interface().(error)
				return err
			}
			return nil
		}
	}

	// Fall back to interface assertion for internal close method
	if closer, ok := cs.(interface{ close() error }); ok {
		return closer.close()
	}

	return fmt.Errorf("ChunkSource does not implement close method")
}

func getChunkCount(cs nbs.ChunkSource) (uint32, error) {
	// First try reflection to call exported Count method
	v := reflect.ValueOf(cs)
	method := v.MethodByName("Count")

	if method.IsValid() {
		// Call the method
		results := method.Call([]reflect.Value{})

		// The Count method should return (uint32, error)
		if len(results) == 2 {
			count := results[0].Interface().(uint32)
			if !results[1].IsNil() {
				err := results[1].Interface().(error)
				return 0, err
			}
			return count, nil
		}
	}

	// Fall back to interface assertion for internal count method
	if counter, ok := cs.(interface{ count() (uint32, error) }); ok {
		return counter.count()
	}

	return 0, fmt.Errorf("ChunkSource does not implement count method")
}

func iterateAllChunks(ctx context.Context, cs nbs.ChunkSource, callback func(chunks.Chunk)) error {
	// First try reflection to call exported IterateAllChunks method
	v := reflect.ValueOf(cs)
	method := v.MethodByName("IterateAllChunks")

	if method.IsValid() {
		// Prepare arguments: ctx, callback, stats
		ctxValue := reflect.ValueOf(ctx)
		callbackValue := reflect.ValueOf(callback)
		statsValue := reflect.ValueOf(&nbs.Stats{})

		// Call the method
		results := method.Call([]reflect.Value{ctxValue, callbackValue, statsValue})

		// The IterateAllChunks method should return error
		if len(results) == 1 {
			if !results[0].IsNil() {
				err := results[0].Interface().(error)
				return err
			}
			return nil
		}
	}

	// Fall back to interface assertion for internal iterateAllChunks method
	if iterator, ok := cs.(interface {
		iterateAllChunks(context.Context, func(chunks.Chunk), *nbs.Stats) error
	}); ok {
		return iterator.iterateAllChunks(ctx, callback, &nbs.Stats{})
	}

	return fmt.Errorf("ChunkSource does not implement iterateAllChunks method")
}

func iterateAllChunksFast(ctx context.Context, cs nbs.ChunkSource, callback func(hash.Hash, chunks.Chunk) error) error {
	// Call the interface method directly - no reflection or type assertions needed!
	stats := &nbs.Stats{}
	return cs.IterateAllChunksFast(ctx, callback, stats)
}
