// Copyright 2020 Dolthub, Inc.
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

package nbs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/dolt/go/store/hash"
)

type gcErrAccum map[string]error

var _ error = gcErrAccum{}

func (ea gcErrAccum) add(path string, err error) {
	ea[path] = err
}

func (ea gcErrAccum) isEmpty() bool {
	return len(ea) == 0
}

func (ea gcErrAccum) Error() string {
	var sb strings.Builder
	sb.WriteString("error garbage collecting the following files:")
	for filePath, err := range ea {
		sb.WriteString(fmt.Sprintf("\t%s: %s", filePath, err.Error()))
	}
	return sb.String()
}

type gcCopier struct {
	writer GenericTableWriter
	tfp    tableFilePersister
}

func newGarbageCollectionCopier(tfp tableFilePersister) (*gcCopier, error) {
	var writer GenericTableWriter
	var err error
	if os.Getenv("DOLT_ARCHIVE_PULL_STREAMER") != "" {
		writer, err = NewArchiveStreamWriter("")
	} else {
		writer, err = NewCmpChunkTableWriter("")
	}
	if err != nil {
		return nil, err
	}
	return &gcCopier{writer, tfp}, nil
}

func (gcc *gcCopier) addChunk(ctx context.Context, c ToChunker) error {
	_, err := gcc.writer.AddChunk(c)
	return err
}

// If the writer should be closed and deleted, instead of being used with
// copyTablesToDir, call this method.
func (gcc *gcCopier) cancel(_ context.Context) error {
	return gcc.writer.Cancel()
}

func (gcc *gcCopier) copyTablesToDir(ctx context.Context) (ts []tableSpec, err error) {
	var filename string
	_, filename, err = gcc.writer.Finish()
	if err != nil {
		return nil, err
	}

	defer func() {
		gcc.writer.Cancel()
	}()

	if gcc.writer.ChunkCount() == 0 {
		return []tableSpec{}, nil
	}

	addr, ok := fileNameToAddr(filename)
	if !ok {
		return nil, fmt.Errorf("invalid filename: %s", filename)
	}

	exists, err := gcc.tfp.Exists(ctx, filename, uint32(gcc.writer.ChunkCount()), nil)
	if err != nil {
		return nil, err
	}

	if exists {
		return []tableSpec{
			{
				name:       addr,
				chunkCount: uint32(gcc.writer.ChunkCount()),
			},
		}, nil
	}

	// Attempt to rename the file to the destination if we are working with a fsTablePersister...
	if mover, ok := gcc.tfp.(movingTableFilePersister); ok {
		err = mover.TryMoveCmpChunkTableWriter(ctx, filename, gcc.writer)
		if err == nil {
			return []tableSpec{
				{
					name:       addr,
					chunkCount: uint32(gcc.writer.ChunkCount()),
				},
			}, nil
		}
	}

	// Otherwise, write the file through CopyTableFile.
	r, err := gcc.writer.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	sz := gcc.writer.FullLength()

	err = gcc.tfp.CopyTableFile(ctx, r, filename, sz, uint32(gcc.writer.ChunkCount()))
	if err != nil {
		return nil, err
	}

	return []tableSpec{
		{
			name:       addr,
			chunkCount: uint32(gcc.writer.ChunkCount()),
		},
	}, nil
}

func fileNameToAddr(fileName string) (hash.Hash, bool) {
	if len(fileName) == 32 {
		addr, ok := hash.MaybeParse(fileName)
		if ok {
			return addr, true
		}
	}
	if len(fileName) == 32+len(ArchiveFileSuffix) && strings.HasSuffix(fileName, ArchiveFileSuffix) {
		addr, ok := hash.MaybeParse(fileName[:32])
		if ok {
			return addr, true
		}
	}
	return hash.Hash{}, false
}
