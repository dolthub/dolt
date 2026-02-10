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

package git

import (
	"os"
	"path/filepath"
)

// NewTempIndex creates a unique temporary git index file (for use as GIT_INDEX_FILE).
// The index is created outside of any repo's GIT_DIR to avoid read-only repos and to
// avoid leaving scratch files in the repo on crashes.
//
// Note: git may also create a sibling lock file (<index>.lock) during index writes.
func NewTempIndex() (dir, indexFile string, cleanup func(), err error) {
	f, err := os.CreateTemp("", "dolt-git-index-")
	if err != nil {
		return "", "", nil, err
	}
	indexFile = f.Name()
	_ = f.Close()
	dir = filepath.Dir(indexFile)
	cleanup = func() {
		_ = os.Remove(indexFile)
		_ = os.Remove(indexFile + ".lock")
	}
	return dir, indexFile, cleanup, nil
}
