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

package remotestorage

import (
	"context"
	"errors"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// RepoFormat is how a DoltChunkStore reads and writes the records of a remote repository. Dolt stores them as they
// are; see PlainFormat.
type RepoFormat interface {
	// ClientFormat returns the format to send in place of |f|, which a server creates a missing repository in.
	ClientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat
	// Opened returns the format of the repository |md| describes, or an error if it cannot be used. A store keeps the
	// format it opened with, and calls Opened on it again only to check the metadata of a refresh.
	Opened(md *remotesapi.GetRepoMetadataResponse) (RepoFormat, error)
	// WriteTableFile returns the table file of |chnks| to upload.
	WriteTableFile(chnks []chunks.Chunk) (name string, data []byte, splitOffset uint64, err error)
	// OpenChunk returns chunk |h| from the |data| of a range located with record key |key|, compressed with |dict|
	// if it is not nil.
	OpenChunk(ctx context.Context, h hash.Hash, key []byte, data []byte, dict *nbs.DecompBundle) (nbs.ToChunker, error)
	// OpenDictionary returns the dictionary in the |data| of a range located with record key |key|.
	OpenDictionary(ctx context.Context, key []byte, data []byte) (*nbs.DecompBundle, error)
}

// PlainFormat is the RepoFormat of repositories whose records are stored as they are.
type PlainFormat struct{}

var _ RepoFormat = PlainFormat{}

var errUnexpectedKey = errors.New("server returned a record key for a repository without them")

func (PlainFormat) ClientFormat(f *remotesapi.ClientRepoFormat) *remotesapi.ClientRepoFormat {
	return f
}

func (PlainFormat) Opened(*remotesapi.GetRepoMetadataResponse) (RepoFormat, error) {
	return PlainFormat{}, nil
}

func (PlainFormat) WriteTableFile(chnks []chunks.Chunk) (string, []byte, uint64, error) {
	return nbs.WriteChunks(chnks)
}

func (PlainFormat) OpenChunk(_ context.Context, h hash.Hash, key []byte, data []byte, dict *nbs.DecompBundle) (nbs.ToChunker, error) {
	if key != nil {
		return nil, errUnexpectedKey
	}
	if dict != nil {
		return nbs.NewArchiveToChunker(h, dict, data), nil
	}
	return nbs.NewCompressedChunk(h, data)
}

func (PlainFormat) OpenDictionary(_ context.Context, key []byte, data []byte) (*nbs.DecompBundle, error) {
	if key != nil {
		return nil, errUnexpectedKey
	}
	return nbs.NewDecompBundle(data)
}

func repoFormatOf(opts []RepoFormat) RepoFormat {
	if len(opts) == 0 || opts[0] == nil {
		return PlainFormat{}
	}
	return opts[0]
}
