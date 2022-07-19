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

package tempfiles

import (
	"context"
	"os"
)

// TempFileProvider is an interface which provides methods for creating temporary files.
type TempFileProvider interface {
	// GetTempDir returns the directory where temp files will be created by default
	GetTempDir() string

	// NewFile creates a new temporary file in the directory dir, opens the file for reading and writing, and returns
	// the resulting *os.File. If dir is "" then the default temp dir is used.
	NewFile(dir, pattern string) (*os.File, error)

	// Clean makes a best effort attempt to delete all temp files created by calls to NewFile
	Clean()
}

const tempFileBufferSize = 1024

// TempFileProviderAt is a TempFileProvider interface which creates temp files at a given path.
type TempFileProviderAt struct {
	ctx     context.Context
	stop    context.CancelFunc
	tempDir string
	prefix  string
	files   chan *os.File
	errs    chan error
}

// NewTempFileProviderAt creates a new TempFileProviderAt instance with the provided directory to create files in. The
// directory is assumed to have been created already.
func NewTempFileProviderAt(tempDir string, prefix string) *TempFileProviderAt {
	f := make(chan *os.File, tempFileBufferSize)
	e := make(chan error, tempFileBufferSize)
	fp := &TempFileProviderAt{files: f, errs: e, tempDir: tempDir, prefix: prefix}
	return fp
}

// GetTempDir returns the directory where temp files will be created by default
func (tfp *TempFileProviderAt) GetTempDir() string {
	return tfp.tempDir
}

func (tfp *TempFileProviderAt) Run(ctx context.Context) {
	var childCtx context.Context
	childCtx, tfp.stop = context.WithCancel(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			f, err := os.CreateTemp(tfp.tempDir, tfp.prefix)
			tfp.files <- f
			tfp.errs <- err
		}
	}(childCtx)
}

// NewFile creates a new temporary file in the directory dir, opens the file for reading and writing, and returns
// the resulting *os.File. If dir is "" then the default temp dir is used.
func (tfp *TempFileProviderAt) NewFile(dir, pattern string) (*os.File, error) {
	f := <-tfp.files
	err := <-tfp.errs

	return f, err
}

// Clean makes a best effort attempt to delete all temp files created by calls to NewFile
func (tfp *TempFileProviderAt) Clean() {
	if tfp.stop != nil {
		tfp.stop()
		go func() {
			for {
				select {
				case <-tfp.files:
				case <-tfp.errs:
				default:
					return
				}
			}
			close(tfp.files)
			close(tfp.errs)
		}()
	}
}

// MovableTemFile is an object that implements TempFileProvider that is used by the nbs to create temp files that
// ultimately will be renamed.  It is important to use this instance rather than using os.TempDir, or os.CreateTemp
// directly as those may have errors executing a rename against if the volume the default temporary directory lives on
// is different than the volume of the destination of the rename.
var MovableTempFileProvider TempFileProvider = NewTempFileProviderAt(os.TempDir(), "")
