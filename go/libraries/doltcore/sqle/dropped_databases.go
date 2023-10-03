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

package sqle

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// deletedDatabaseDirectoryName is the subdirectory within the data folder where Dolt moves databases after they are
// dropped. The dolt_undrop() stored procedure is then able to restore them from this location.
const deletedDatabaseDirectoryName = "dolt_deleted_databases"

// TODO: Godoc
type droppedDatabases struct {
	fs filesys.Filesys
}

func newDroppedDatabaseManager(fs filesys.Filesys) *droppedDatabases {
	return &droppedDatabases{
		fs: fs,
	}
}

func (dd *droppedDatabases) DropDatabase(ctx *sql.Context, name string, dropDbLoc string) error {
	rootDbLoc, err := dd.fs.Abs("")
	if err != nil {
		return err
	}

	isRootDatabase := false
	//dirToDelete := ""
	// if the database is in the directory itself, we remove '.dolt' directory rather than
	// the whole directory itself because it can have other databases that are nested.
	if rootDbLoc == dropDbLoc {
		doltDirExists, _ := dd.fs.Exists(dbfactory.DoltDir)
		if !doltDirExists {
			return sql.ErrDatabaseNotFound.New(name)
		}
		dropDbLoc = filepath.Join(dropDbLoc, dbfactory.DoltDir)
		isRootDatabase = true
	} else {
		// TODO: Do we really need the code in this block?
		//       Seems like a few places are checking this.
		exists, isDir := dd.fs.Exists(dropDbLoc)
		// Get the DB's directory
		if !exists {
			// engine should already protect against this
			return sql.ErrDatabaseNotFound.New(name)
		} else if !isDir {
			return fmt.Errorf("unexpected error: %s exists but is not a directory", name)
		}
	}

	if err = dd.initializeDeletedDatabaseDirectory(); err != nil {
		return fmt.Errorf("unable to drop database %s: %w", name, err)
	}

	// Move the dropped database to the Dolt deleted database directory so it can be restored if needed
	_, file := filepath.Split(dropDbLoc)
	var destinationDirectory string
	if isRootDatabase {
		// NOTE: This won't work without first creating the new subdirectory
		newSubdirectory := filepath.Join(deletedDatabaseDirectoryName, name)
		// TODO: If newSubdirectory exists already... then we're in trouble! (need to handle that)
		// TODO: Maybe we should have this talk to the DroppedDatabaseVault API instead?
		if err := dd.fs.MkDirs(newSubdirectory); err != nil {
			return err
		}
		destinationDirectory = filepath.Join(newSubdirectory, file)
	} else {
		destinationDirectory = filepath.Join(deletedDatabaseDirectoryName, file)
	}

	// Add the final directory segment and convert all hyphens to underscores in the database directory name
	dir, file := filepath.Split(destinationDirectory)
	if strings.Contains(file, "-") {
		destinationDirectory = filepath.Join(dir, strings.ReplaceAll(file, "-", "_"))
	}

	if err := dd.prepareToMoveDroppedDatabase(ctx, destinationDirectory); err != nil {
		return err
	}
	return dd.fs.MoveDir(dropDbLoc, destinationDirectory)
}

func (dd *droppedDatabases) UndropDatabase(ctx *sql.Context, name string) (filesys.Filesys, string, error) {
	// TODO: not sure I like sourcePath and destinationPath being returned here, but seems like they're needed in this function
	sourcePath, destinationPath, exactCaseName, err := dd.validateUndropDatabase(ctx, name)
	if err != nil {
		return nil, "", err
	}

	err = dd.fs.MoveDir(sourcePath, destinationPath)
	if err != nil {
		return nil, "", err
	}

	newFs, err := dd.fs.WithWorkingDir(exactCaseName)
	if err != nil {
		return nil, "", err
	}

	return newFs, exactCaseName, nil
}

// initializeDeletedDatabaseDirectory initializes the special directory Dolt uses to store dropped databases until
// they are fully removed. If the directory is already created and set up correctly, then this method is a no-op.
// If the directory doesn't exist yet, it will be created. If there are any problems initializing the directory, an
// error is returned.
func (dd *droppedDatabases) initializeDeletedDatabaseDirectory() error {
	exists, isDir := dd.fs.Exists(deletedDatabaseDirectoryName)
	if exists && !isDir {
		return fmt.Errorf("%s exists, but is not a directory", deletedDatabaseDirectoryName)
	}

	if exists {
		return nil
	}

	return dd.fs.MkDirs(deletedDatabaseDirectoryName)
}

func (dd *droppedDatabases) ListUndroppableDatabases(_ *sql.Context) ([]string, error) {
	if err := dd.initializeDeletedDatabaseDirectory(); err != nil {
		return nil, fmt.Errorf("unable to list undroppable database: %w", err)
	}

	databaseNames := make([]string, 0, 5)
	callback := func(path string, size int64, isDir bool) (stop bool) {
		_, lastPathSegment := filepath.Split(path)
		// TODO: Is there a common util we use for this somewhere?
		lastPathSegment = strings.ReplaceAll(lastPathSegment, "-", "_")
		databaseNames = append(databaseNames, lastPathSegment)
		return false
	}

	if err := dd.fs.Iter(deletedDatabaseDirectoryName, false, callback); err != nil {
		return nil, err
	}

	return databaseNames, nil
}

// validateUndropDatabase validates that the database |name| is available to be "undropped" and that no existing
// database is already being managed that has the same (case-insensitive) name. If any problems are encountered,
// an error is returned.
func (dd *droppedDatabases) validateUndropDatabase(ctx *sql.Context, name string) (sourcePath, destinationPath, exactCaseName string, err error) {
	// TODO: rename to ListDatabasesThatCanBeUndropped(ctx)?
	availableDatabases, err := dd.ListUndroppableDatabases(ctx)
	if err != nil {
		return "", "", "", err
	}

	found := false
	exactCaseName = name
	lowercaseName := strings.ToLower(name)
	for _, s := range availableDatabases {
		if lowercaseName == strings.ToLower(s) {
			exactCaseName = s
			found = true
			break
		}
	}

	if !found {
		return "", "", "", fmt.Errorf("no database named '%s' found to undrop. %s",
			name, errors.CreateUndropErrorMessage(availableDatabases))
	}

	// Check to see if the destination directory for restoring the database already exists (case-insensitive match)
	destinationPath, err = dd.fs.Abs(exactCaseName)
	if err != nil {
		return "", "", "", err
	}

	sourcePath = filepath.Join(deletedDatabaseDirectoryName, exactCaseName)

	found = false
	dd.fs.Iter(filepath.Dir(destinationPath), false, func(path string, size int64, isDir bool) (stop bool) {
		if strings.ToLower(filepath.Base(path)) == strings.ToLower(filepath.Base(destinationPath)) {
			found = true
		}
		return found
	})

	if found {
		return "", "", "", fmt.Errorf("unable to undrop database '%s'; "+
			"another database already exists with the same case-insensitive name", exactCaseName)
	}

	return sourcePath, destinationPath, exactCaseName, nil
}

func (dd *droppedDatabases) prepareToMoveDroppedDatabase(_ *sql.Context, targetPath string) error {
	if exists, _ := dd.fs.Exists(targetPath); !exists {
		// If there's nothing at the desired targetPath, we're all set
		return nil
	}

	// If there is something already there, pick a new path to move it to
	newPath := fmt.Sprintf("%s.backup.%d", targetPath, time.Now().Unix())
	if exists, _ := dd.fs.Exists(newPath); exists {
		return fmt.Errorf("unable to move existing dropped database out of the way: "+
			"tried to move it to %s", newPath)
	}
	if err := dd.fs.MoveFile(targetPath, newPath); err != nil {
		return fmt.Errorf("unable to move existing dropped database out of the way: %w", err)
	}

	return nil
}
