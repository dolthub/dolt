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

package tempfiles

import (
	"io/ioutil"
	"os"
	"sync"
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

// TempFileProviderAt is a TempFileProvider interface which creates temp files at a given path.
type TempFileProviderAt struct {
	tempDir      string
	filesCreated []string
	mu           sync.Mutex
}

// NewTempFileProviderAt creates a new TempFileProviderAt instance with the provided directory to create files in. The
// directory is assumed to have been created already.
func NewTempFileProviderAt(tempDir string) *TempFileProviderAt {
	return &TempFileProviderAt{tempDir, nil, sync.Mutex{}}
}

// GetTempDir returns the directory where temp files will be created by default
func (tfp *TempFileProviderAt) GetTempDir() string {
	return tfp.tempDir
}

// NewFile creates a new temporary file in the directory dir, opens the file for reading and writing, and returns
// the resulting *os.File. If dir is "" then the default temp dir is used.
func (tfp *TempFileProviderAt) NewFile(dir, pattern string) (*os.File, error) {
	tfp.mu.Lock()
	defer tfp.mu.Unlock()
	if dir == "" {
		dir = tfp.tempDir
	}

	f, err := ioutil.TempFile(dir, pattern)

	if err == nil {
		tfp.filesCreated = append(tfp.filesCreated, f.Name())
	}

	return f, err
}

// Clean makes a best effort attempt to delete all temp files created by calls to NewFile
func (tfp *TempFileProviderAt) Clean() {
	tfp.mu.Lock()
	defer tfp.mu.Unlock()
	for _, filename := range tfp.filesCreated {
		// best effort. ignore errors
		_ = os.Remove(filename)
	}
}

// MovableTemFile is an object that implements TempFileProvider that is used by the nbs to create temp files that
// ultimately will be renamed.  It is important to use this instance rather than using os.TempDir, or ioutil.TempFile
// directly as those may have errors executing a rename against if the volume the default temporary directory lives on
// is different than the volume of the destination of the rename.
var MovableTempFileProvider TempFileProvider = NewTempFileProviderAt(os.TempDir())
