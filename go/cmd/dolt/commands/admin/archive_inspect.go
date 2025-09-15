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

package admin

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

func (cmd ArchiveInspectCmd) RequiresRepo() bool {
	return false
}

func (cmd ArchiveInspectCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd ArchiveInspectCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.SupportsString("archive-path", "", "archive_path", "Full path to the archive file (.darc) to inspect")
	ap.SupportsFlag("mmap", "", "Enable memory-mapped index reading for better performance")
	ap.SupportsString("object-id", "", "object_id", "Base32-encoded 20-byte object ID to inspect within the archive")
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

	// Check if file exists
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		cli.PrintErrln("Error: Archive file does not exist:", archivePath)
		return 1
	}

	// Check if file has .darc extension
	if !strings.HasSuffix(strings.ToLower(archivePath), nbs.ArchiveFileSuffix) {
		cli.PrintErrln("Warning: File does not have .darc extension")
	}

	// Make path absolute
	absPath, err := filepath.Abs(archivePath)
	if err != nil {
		cli.PrintErrln("Error getting absolute path:", err.Error())
		return 1
	}

	// Check for mmap flag
	enableMmap := apr.Contains("mmap")

	// Create archive inspector
	inspector, err := nbs.NewArchiveInspectorFromFileWithMmap(ctx, absPath, enableMmap)
	if err != nil {
		cli.PrintErrln("Error opening archive file:", err.Error())
		return 1
	}
	defer inspector.Close()

	// Display basic archive information
	cli.Println("Archive file:", absPath)
	cli.Println("Archive loaded successfully!")
	cli.Println()
	
	// Basic file information
	cli.Printf("File size: %d bytes\n", inspector.FileSize())
	cli.Printf("Format version: %d\n", inspector.FormatVersion())
	cli.Printf("File signature: %s\n", inspector.FileSignature())
	cli.Println()
	
	// Archive structure information
	cli.Printf("Chunk count: %d\n", inspector.ChunkCount())
	cli.Printf("Byte span count: %d\n", inspector.ByteSpanCount())
	cli.Printf("Index size: %d bytes\n", inspector.IndexSize())
	cli.Printf("Metadata size: %d bytes\n", inspector.MetadataSize())
	
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
				// If not JSON, just print raw bytes
				cli.Printf("  %s\n", string(metadataBytes))
			}
		}
	}

	// Handle object-id inspection if provided
	if objectIdStr, ok := apr.GetValue("object-id"); ok {
		cli.Println()
		cli.Println("Object inspection:")
		
		// Parse the hash
		objectHash, hashOk := hash.MaybeParse(objectIdStr)
		if !hashOk {
			cli.PrintErrln("Error: Invalid object ID format. Expected 32-character base32 encoded hash.")
			return 1
		}
		
		// Look up the object in the archive
		chunkInfo, err := inspector.GetChunkInfo(ctx, objectHash)
		if err != nil {
			cli.PrintErrln("Error inspecting object:", err.Error())
			return 1
		}
		
		if chunkInfo == nil {
			cli.Printf("Object %s not found in archive\n", objectIdStr)
		} else {
			cli.Printf("Object ID: %s\n", objectIdStr)
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

	return 0
}