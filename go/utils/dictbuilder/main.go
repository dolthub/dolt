// Copyright 2026 Dolthub, Inc.
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
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/gozstd"
)

const (
	chunksPerDB = 1000
	dictSize    = 1 << 14 // 16KB
)

func main() {
	dbPaths := os.Args[1:]

	if len(dbPaths) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s <path1> <path2> ...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "At least one database path must be specified\n")
		os.Exit(1)
	}

	ctx := context.Background()
	var allSamples [][]byte

	for _, dbPath := range dbPaths {
		fmt.Fprintf(os.Stderr, "Processing database: %s\n", dbPath)
		samples, err := extractChunks(ctx, dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", dbPath, err)
			continue
		}
		allSamples = append(allSamples, samples...)
		fmt.Fprintf(os.Stderr, "Extracted %d chunks from %s\n", len(samples), dbPath)
	}

	if len(allSamples) == 0 {
		fmt.Fprintf(os.Stderr, "No chunks extracted from any database\n")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Building dictionary from %d total samples...\n", len(allSamples))

	dict := gozstd.BuildDict(allSamples, dictSize)

	fmt.Fprintf(os.Stderr, "Generated dictionary of size: %d bytes\n", len(dict))

	// Output as Go code
	fmt.Printf("var staticDictionary = []byte{\n")
	for i, b := range dict {
		if i%16 == 0 {
			fmt.Printf("    ")
		}
		fmt.Printf("0x%02X", b)
		if i < len(dict)-1 {
			fmt.Printf(", ")
		}
		if i%16 == 15 {
			fmt.Printf("\n")
		}
	}
	if len(dict)%16 != 0 {
		fmt.Printf("\n")
	}
	fmt.Printf("}\n")
}

func extractChunks(ctx context.Context, dbPath string) ([][]byte, error) {
	if !isDoltDB(dbPath) {
		return nil, fmt.Errorf("path does not appear to be a dolt database: %s", dbPath)
	}

	chunkStore, err := openDoltChunkStore(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk store: %v", err)
	}
	defer chunkStore.Close()

	var samples [][]byte
	count := 0

	err = chunkStore.IterateAllChunks(ctx, func(chunk chunks.Chunk) {
		if count >= chunksPerDB {
			return
		}

		data := chunk.Data()
		if len(data) > 0 {
			sample := make([]byte, len(data))
			copy(sample, data)
			samples = append(samples, sample)
			count++
		}
	})

	if err != nil {
		return nil, fmt.Errorf("error iterating chunks: %v", err)
	}

	return samples, nil
}

func openDoltChunkStore(ctx context.Context, dbPath string) (*nbs.GenerationalNBS, error) {
	newGenPath := filepath.Join(dbPath, ".dolt", "noms")
	oldGenPath := filepath.Join(dbPath, ".dolt", "oldgen")

	const mmapArchiveIndexes = false
	quotaProvider := nbs.NewUnlimitedMemQuotaProvider()

	newGenSt, err := nbs.NewLocalJournalingStore(ctx, "", newGenPath, quotaProvider, mmapArchiveIndexes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create new generation store: %v", err)
	}

	var oldGenSt *nbs.NomsBlockStore
	if _, err := os.Stat(oldGenPath); os.IsNotExist(err) {
		if err := os.MkdirAll(oldGenPath, 0755); err != nil {
			newGenSt.Close()
			return nil, fmt.Errorf("failed to create oldgen directory: %v", err)
		}
	}
	oldGenSt, err = nbs.NewLocalStore(ctx, newGenSt.Version(), oldGenPath, 256*1024*1024, quotaProvider, mmapArchiveIndexes)
	if err != nil {
		newGenSt.Close()
		return nil, fmt.Errorf("failed to create empty old generation store: %v", err)
	}

	ghostGen, err := nbs.NewGhostBlockStore(dbPath)
	if err != nil {
		newGenSt.Close()
		oldGenSt.Close()
		return nil, fmt.Errorf("failed to create ghost generation store: %v", err)
	}

	chunkStore := nbs.NewGenerationalCS(oldGenSt, newGenSt, ghostGen)

	return chunkStore, nil
}

// isDoltDB checks if the given path appears to be a dolt database
func isDoltDB(path string) bool {
	// Look for .dolt directory
	doltDir := filepath.Join(path, ".dolt")
	if info, err := os.Stat(doltDir); err == nil && info.IsDir() {
		return true
	}

	// Also check if the path itself is a .dolt directory
	if strings.HasSuffix(path, ".dolt") {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return true
		}
	}

	return false
}
