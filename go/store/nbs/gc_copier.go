// Copyright 2020 Liquidata, Inc.
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
	"path"
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

func (gcc *gcCopier) copyTablesToDir(ctx context.Context, destDir string) ([]tableSpec, error) {
	filename, err := gcc.writer.Finish()
	if err != nil {
		return nil, err
	}

	filepath := path.Join(destDir, filename)
	err = gcc.writer.FlushToFile(filepath)
	if err != nil {
		return nil, err
	}

	addr, err := parseAddr(filename)
	if err != nil {
		return nil, err
	}

	return []tableSpec{
		tableSpec{
			name:       addr,
			chunkCount: gcc.writer.ChunkCount(),
		},
	}, nil
}
