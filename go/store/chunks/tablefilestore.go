// Copyright 2023 Dolthub, Inc.
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

package chunks

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/hash"
)

const JournalFileID = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"

// TableFile is an interface for working with an existing table file
type TableFile interface {
	// FileID gets the id of the file
	FileID() string

	// LocationPrefix
	LocationPrefix() string

	// NumChunks returns the number of chunks in a table file
	NumChunks() int

	// Open returns an io.ReadCloser which can be used to read the bytes of a
	// table file. It also returns the content length of the table file.
	Open(ctx context.Context) (io.ReadCloser, uint64, error)
}

// Describes what is possible to do with TableFiles in a TableFileStore.
type TableFileStoreOps struct {
	// True is the TableFileStore supports reading table files.
	CanRead bool
	// True is the TableFileStore supports writing table files.
	CanWrite bool
	// True is the TableFileStore supports pruning unused table files.
	CanPrune bool
	// True is the TableFileStore supports garbage collecting chunks.
	CanGC bool
}

// TableFileStore is an interface for interacting with table files directly
type TableFileStore interface {
	// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files),
	// and a second list containing only appendix table files.
	Sources(ctx context.Context) (hash.Hash, []TableFile, []TableFile, error)

	// Size  returns the total size, in bytes, of the table files in this Store.
	Size(ctx context.Context) (uint64, error)

	// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore.
	WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error

	// AddTableFilesToManifest adds table files to the manifest
	AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int, getAddrs GetAddrsCurry) error

	// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
	PruneTableFiles(ctx context.Context) error

	// SetRootChunk changes the root chunk hash from the previous value to the new root.
	SetRootChunk(ctx context.Context, root, previous hash.Hash) error

	// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
	SupportedOperations() TableFileStoreOps
}
