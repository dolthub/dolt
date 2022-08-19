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
	"io"
	"os"
	"path"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
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

func (gcc *gcCopier) copyTablesToDir(ctx context.Context, destDir string) (ts []tableSpec, err error) {
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

	filepath := path.Join(destDir, filename)

	var addr addr
	addr, err = parseAddr(filename)
	if err != nil {
		return nil, err
	}

	if info, err := os.Stat(filepath); err == nil {
		// file already exists
		if gcc.writer.ContentLength() != uint64(info.Size()) {
			return nil, fmt.Errorf("'%s' already exists with different contents.", filepath)
		}
		return []tableSpec{
			{
				name:       addr,
				chunkCount: uint32(gcc.writer.ChunkCount()),
			},
		}, nil
	}

	// Otherwise, write the file.
	var tf string
	tf, err = func() (tf string, err error) {
		var temp *os.File
		temp, err = tempfiles.MovableTempFileProvider.NewFile(destDir, tempTablePrefix)
		if err != nil {
			return "", err
		}
		defer func() {
			cerr := temp.Close()
			if err == nil {
				err = cerr
			}
		}()

		r, err := gcc.writer.Reader()
		if err != nil {
			return "", err
		}
		defer func() {
			cerr := r.Close()
			if err == nil {
				err = cerr
			}
		}()

		_, err = io.Copy(temp, r)
		if err != nil {
			return "", err
		}

		return temp.Name(), nil
	}()
	if err != nil {
		return nil, err
	}

	err = file.Rename(tf, filepath)
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
