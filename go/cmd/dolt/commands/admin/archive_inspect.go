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
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type ArchiveInspectCmd struct {
}

func (cmd ArchiveInspectCmd) Name() string {
	return "archive-inspect"
}

func (cmd ArchiveInspectCmd) Description() string {
	return "Inspect a Dolt archive (.darc) file and display basic information about it."
}

func (cmd ArchiveInspectCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		ShortDesc: "Inspect a Dolt archive (.darc) file and display information about it",
		LongDesc: `Inspects a Dolt archive (.darc) file and displays detailed information about its structure, contents, and metadata.

Archive files are compressed collections of chunks used by Dolt for storage. This command provides debugging and inspection capabilities for these files.

This command takes a path to an archive file, and ignores any database information that would otherwise be provided. To skip wasting time, run this command outside of a Dolt repository.'

Basic usage displays archive metadata, structure information, and statistics. Advanced usage allows inspection of specific chunks by object ID or raw index positions.`,
		Synopsis: []string{
			"[--mmap] <archive-path>",
			"[--mmap] --object-id <hash> <archive-path>",
			"[--mmap] --inspect-index <index> <archive-path>",
		},
	}
}

func (cmd ArchiveInspectCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.SupportsString("archive-path", "", "archive_path", "Path to the archive file (.darc) to inspect")
	ap.SupportsFlag("mmap", "", "Enable memory-mapped index reading. Default is to load index into memory.")
	ap.SupportsString("object-id", "", "object_id", "Base32-encoded 20-byte object ID to inspect within the archive")
	ap.SupportsString("inspect-index", "", "index", "Inspect raw index reader data at specific index position")
	return ap
}

func (cmd ArchiveInspectCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, usage)

	var archivePath string
	if archivePathArg, ok := apr.GetValue("archive-path"); ok {
		archivePath = archivePathArg
	} else if apr.NArg() == 1 {
		archivePath = apr.Arg(0)
	} else {
		usage()
		return 1
	}

	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		cli.PrintErrln("Error: Archive file does not exist:", archivePath)
		return 1
	}
	if !strings.HasSuffix(strings.ToLower(archivePath), nbs.ArchiveFileSuffix) {
		cli.PrintErrln("Warning: File does not have .darc extension")
	}

	absPath, err := filepath.Abs(archivePath)
	if err != nil {
		cli.PrintErrln("Error getting absolute path:", err.Error())
		return 1
	}

	enableMmap := apr.Contains("mmap")
	inspector, err := nbs.NewArchiveInspectorFromFileWithMmap(ctx, absPath, nbs.NewUnlimitedMemQuotaProvider(), enableMmap)
	if err != nil {
		cli.PrintErrln("Error opening archive file:", err.Error())
		return 1
	}
	defer inspector.Close()

	cli.Println("Archive file:", absPath)
	cli.Printf("File size: %d bytes\n", inspector.FileSize())
	cli.Printf("Format version: %d\n", inspector.FormatVersion())
	cli.Printf("File signature: %s\n", inspector.FileSignature())
	cli.Printf("Chunk count: %d\n", inspector.ChunkCount())
	cli.Printf("Byte span count: %d\n", inspector.ByteSpanCount())
	cli.Printf("Index size: %d bytes\n", inspector.IndexSize())
	cli.Printf("Metadata size: %d bytes\n", inspector.MetadataSize())
	cli.Printf("Split offset: %d bytes\n", inspector.SplitOffset())

	// Display metadata if present
	if inspector.MetadataSize() > 0 {
		cli.Println()
		cli.Println("Metadata:")
		metadataBytes, err := inspector.GetMetadata(ctx)
		if err != nil {
			cli.PrintErrln("Error reading metadata:", err.Error())
		} else {
			// Try to parse as JSON and pretty print
			var metadataObj interface{}
			if err := json.Unmarshal(metadataBytes, &metadataObj); err == nil {
				prettyJSON, _ := json.MarshalIndent(metadataObj, "  ", "  ")
				cli.Printf("  %s\n", string(prettyJSON))
			} else {
				// If not JSON, just print as ascii. To date we don't have any non-JSON metadata.
				cli.Printf("  %s\n", string(metadataBytes))
			}
		}
	} else {
		cli.Println("Metadata: none")
	}

	if objectIdStr, ok := apr.GetValue("object-id"); ok {
		cli.Println()
		cli.Println("Object inspection:")

		objectHash, hashOk := hash.MaybeParse(objectIdStr)
		if !hashOk {
			cli.PrintErrln("Error: Invalid object ID format. Expected 32-character base32 encoded hash.")
			return 1
		}

		debugInfo := inspector.SearchChunkDebug(objectHash)

		cli.Printf("Hash: %s\n", debugInfo.Hash)
		cli.Printf("Prefix: 0x%x\n", debugInfo.Prefix)
		cli.Printf("Suffix: 0x%x\n", debugInfo.Suffix)
		cli.Printf("Index reader type: %s\n", debugInfo.IndexReaderType)
		cli.Printf("Chunk count: %d\n", debugInfo.ChunkCount)
		cli.Printf("Possible match index: %d\n", debugInfo.PossibleMatch)
		cli.Printf("Valid range: %t\n", debugInfo.ValidRange)
		cli.Printf("Final search result: %d\n", debugInfo.FinalResult)

		cli.Printf("Prefix matches found: %d\n", len(debugInfo.Matches))
		for i, match := range debugInfo.Matches {
			cli.Printf("  Match %d: index=%d, suffixMatch=%t, suffix=0x%x\n",
				i, match.Index, match.SuffixMatch, match.SuffixAtIdx)
		}
		cli.Println()

		// Look up the object in the archive
		chunkInfo, err := inspector.GetChunkInfo(ctx, objectHash)
		if err != nil {
			cli.PrintErrln("Error inspecting object:", err.Error())
			return 1
		}

		if chunkInfo == nil {
			cli.Printf("Object %s not found in archive\n", objectIdStr)
		} else {
			cli.Printf("Compression type: %s\n", chunkInfo.CompressionType)
			cli.Printf("Dictionary byte span ID: %d\n", chunkInfo.DictionaryID)
			cli.Printf("Data byte span ID: %d\n", chunkInfo.DataID)

			if chunkInfo.DictionaryByteSpan.Length > 0 {
				cli.Printf("Dictionary byte span: offset=%d, length=%d\n",
					chunkInfo.DictionaryByteSpan.Offset, chunkInfo.DictionaryByteSpan.Length)
			} else {
				cli.Println("Dictionary byte span: none (empty)")
			}

			cli.Printf("Data byte span: offset=%d, length=%d\n",
				chunkInfo.DataByteSpan.Offset, chunkInfo.DataByteSpan.Length)
		}
	}

	// Handle inspect-index if provided
	if indexStr, ok := apr.GetValue("inspect-index"); ok {
		cli.Println()
		cli.Println("Index inspection:")

		// Parse the index
		indexVal, err := strconv.ParseUint(indexStr, 10, 32)
		if err != nil {
			cli.PrintErrln("Error: Invalid index format. Expected unsigned integer.")
			return 1
		}

		idx := uint32(indexVal)
		details := inspector.GetIndexReaderDetails(idx)

		// Print all details
		cli.Printf("Index: %d\n", details.RequestedIndex)
		cli.Printf("Index reader type: %s\n", details.IndexReaderType)
		cli.Printf("Chunk count: %d\n", details.ChunkCount)
		cli.Printf("Byte span count: %d\n", details.ByteSpanCount)

		if details.Error != "" {
			cli.Printf("Error: %s\n", details.Error)
			return 1
		}

		cli.Printf("Hash: %s\n", details.Hash)
		cli.Printf("Prefix: 0x%x\n", details.Prefix)
		cli.Printf("Suffix: 0x%x\n", details.Suffix)
		cli.Printf("Dictionary ID: %d\n", details.DictionaryID)
		cli.Printf("Data ID: %d\n", details.DataID)

		// Show implementation-specific details
		cli.Println()
		cli.Println("Implementation details:")

		// Show common calculation details first
		if details.ExpectedSuffixStart != 0 || details.ExpectedSuffixEnd != 0 {
			cli.Printf("Expected suffix start: %d\n", details.ExpectedSuffixStart)
			cli.Printf("Expected suffix end: %d\n", details.ExpectedSuffixEnd)
		}

		// Show in-memory specific details
		if details.PrefixArrayLength > 0 {
			cli.Printf("Storage type: In-memory arrays\n")
			cli.Printf("Prefix array length: %d\n", details.PrefixArrayLength)
			cli.Printf("Suffix array length: %d\n", details.SuffixArrayLength)
			cli.Printf("Chunk ref array length: %d\n", details.ChunkRefArrayLength)
			cli.Printf("Span index array length: %d\n", details.SpanIndexArrayLength)
			cli.Printf("Suffix array bounds valid: %t\n", details.SuffixArrayBounds)
		}

		// Show mmap specific details
		if details.MmapIndexSize > 0 {
			cli.Printf("Storage type: Memory-mapped file\n")
			cli.Printf("Span index offset: %d\n", details.SpanIndexOffset)
			cli.Printf("Prefixes offset: %d\n", details.PrefixesOffset)
			cli.Printf("Chunk refs offset: %d\n", details.ChunkRefsOffset)
			cli.Printf("Suffixes offset: %d\n", details.SuffixesOffset)
			cli.Printf("Actual suffix file offset: %d\n", details.ActualSuffixOffset)
		}

		// Show raw suffix bytes for both implementations
		if len(details.RawSuffixBytes) > 0 {
			cli.Printf("Raw suffix bytes: %x\n", details.RawSuffixBytes)
		}
		if details.RawSuffixBytesError != "" {
			cli.Printf("Raw suffix bytes error: %s\n", details.RawSuffixBytesError)
		}
	}

	return 0
}
