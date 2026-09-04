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

package dbfactory

import (
	"errors"
	"os"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// safeToIgnoreMarkerFile is the name of the marker file that creation writes into a database directory before
// any other state and removes only once the database is complete. Directory scans skip a directory containing
// this file instead of serving a half-written database.
const safeToIgnoreMarkerFile = ".dolt_safe_to_ignore"

// safeToIgnoreMarkerPerm is the marker's file mode, world-readable so any process scanning the data directory
// can detect it.
const safeToIgnoreMarkerPerm os.FileMode = 0o644

// MarkDatabaseInProgress writes the in-progress marker into the database directory rooted at |dbFS|.
func MarkDatabaseInProgress(dbFS filesys.Filesys) error {
	return dbFS.WriteFile(safeToIgnoreMarkerFile, nil, safeToIgnoreMarkerPerm)
}

// ClearDatabaseInProgress removes the in-progress marker from the database directory rooted at |dbFS|. The
// removal is made durable so a crash cannot resurrect the marker and hide a completed database. Clearing an
// already-absent marker is not an error.
func ClearDatabaseInProgress(dbFS filesys.Filesys) error {
	err := dbFS.DeleteFileDurably(safeToIgnoreMarkerFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// IsDatabaseInProgress reports whether the database directory rooted at |dbFS| carries the in-progress marker.
func IsDatabaseInProgress(dbFS filesys.Filesys) bool {
	marked, _ := dbFS.Exists(safeToIgnoreMarkerFile)
	return marked
}
