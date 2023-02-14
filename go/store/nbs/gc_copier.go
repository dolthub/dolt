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
	"strings"
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
	writer *CmpChunkTableWriter
}

func newGarbageCollectionCopier() (*gcCopier, error) {
	writer, err := NewCmpChunkTableWriter("")
	if err != nil {
		return nil, err
	}
	return &gcCopier{writer}, nil
}

func (gcc *gcCopier) addChunk(ctx context.Context, c CompressedChunk) error {
	return gcc.writer.AddCmpChunk(c)
}

func (gcc *gcCopier) copyTablesToDir(ctx context.Context, tfp tableFilePersister) (ts []tableSpec, err error) {
	var filename string
	filename, err = gcc.writer.Finish()
	if err != nil {
		return nil, err
	}

	if gcc.writer.ChunkCount() == 0 {
		return []tableSpec{}, nil
	}

	defer func() {
		_ = gcc.writer.Remove()
	}()

	var addr addr
	addr, err = parseAddr(filename)
	if err != nil {
		return nil, err
	}

	exists, err := tfp.Exists(ctx, addr, uint32(gcc.writer.ChunkCount()), nil)
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
	if mover, ok := tfp.(movingTableFilePersister); ok {
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
	sz := gcc.writer.ContentLength()

	err = tfp.CopyTableFile(ctx, r, filename, sz, uint32(gcc.writer.ChunkCount()))
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
