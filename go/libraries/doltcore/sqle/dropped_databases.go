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

// droppedDatabaseDirectoryName is the subdirectory within the data folder where Dolt moves databases after they are
// dropped. The dolt_undrop() stored procedure is then able to restore them from this location.
const droppedDatabaseDirectoryName = ".dolt_dropped_databases"

// droppedDatabaseManager is responsible for dropping databases and "undropping", or restoring, dropped databases. It
// is given a Filesys where all database directories can be found. When dropping a database, instead of deleting the
// database directory, it will move it to a new ".dolt_dropped_databases" directory where databases can be restored.
type droppedDatabaseManager struct {
	fs filesys.Filesys
}

// newDroppedDatabaseManager creates a new droppedDatabaseManager instance using the specified |fs| as the location
// where databases can be found. It will create a new ".dolt_dropped_databases" directory at the root of |fs| where
// dropped databases will be moved until they are permanently removed.
func newDroppedDatabaseManager(fs filesys.Filesys) *droppedDatabaseManager {
	return &droppedDatabaseManager{
		fs: fs,
	}
}

// DropDatabase will move the database directory for the database named |name| at the location |dropDbLoc| to the
// dolt_dropped_database directory where it can later be "undropped" to restore it. If any problems are encountered
// moving the database directory, an error is returned.
func (dd *droppedDatabaseManager) DropDatabase(ctx *sql.Context, name string, dropDbLoc string) error {
	rootDbLoc, err := dd.fs.Abs("")
	if err != nil {
		return err
	}

	isRootDatabase := false
	// if the database is in the directory itself, we remove '.dolt' directory rather than
	// the whole directory itself because it can have other databases that are nested.
	if rootDbLoc == dropDbLoc {
		doltDirExists, _ := dd.fs.Exists(dbfactory.DoltDir)
		if !doltDirExists {
			return sql.ErrDatabaseNotFound.New(name)
		}
		dropDbLoc = filepath.Join(dropDbLoc, dbfactory.DoltDir)
		isRootDatabase = true
	}

	if err = dd.initializeDeletedDatabaseDirectory(); err != nil {
		return fmt.Errorf("unable to drop database %s: %w", name, err)
	}

	// Move the dropped database to the Dolt deleted database directory so it can be restored if needed
	_, file := filepath.Split(dropDbLoc)
	var destinationDirectory string
	if isRootDatabase {
		// For a root database, first create the subdirectory before we copy over the .dolt directory
		newSubdirectory := filepath.Join(droppedDatabaseDirectoryName, name)
		if err := dd.fs.MkDirs(newSubdirectory); err != nil {
			return err
		}
		destinationDirectory = filepath.Join(newSubdirectory, file)
	} else {
		destinationDirectory = filepath.Join(droppedDatabaseDirectoryName, file)
	}

	// Add the final directory segment and convert any invalid chars so that the physical directory
	// name matches the current logical/SQL name of the database.
	dir, base := filepath.Split(destinationDirectory)
	base = dbfactory.DirToDBName(file)
	destinationDirectory = filepath.Join(dir, base)

	if err := dd.prepareToMoveDroppedDatabase(ctx, destinationDirectory); err != nil {
		return err
	}

	return dd.fs.MoveDir(dropDbLoc, destinationDirectory)
}

// UndropDatabase will restore the database named |name| by moving it from the dolt_dropped_database directory, back
// into the root of the filesystem where database directories are managed. This function returns the new location of
// the database directory and the exact name (case-sensitive) of the database. If any errors are encountered while
// attempting to undrop the database, an error is returned and other return parameters should be ignored.
func (dd *droppedDatabaseManager) UndropDatabase(ctx *sql.Context, name string) (filesys.Filesys, string, error) {
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

// PurgeAllDroppedDatabases permanently removes all dropped databases that are being held in the dolt_dropped_database
// holding directory. Once dropped databases are purged, they can no longer be restored, so this method should be used
// with caution.
func (dd *droppedDatabaseManager) PurgeAllDroppedDatabases(_ *sql.Context) error {
	// If the dropped database holding directory doesn't exist, then there's nothing to purge
	if exists, _ := dd.fs.Exists(droppedDatabaseDirectoryName); !exists {
		return nil
	}

	var err error
	callback := func(path string, size int64, isDir bool) (stop bool) {
		// Sanity check that the path we're about to delete is under the dropped database holding directory
		if strings.Contains(path, droppedDatabaseDirectoryName) == false {
			err = fmt.Errorf("path of database to purge isn't under dropped database holding directory: %s", path)
			return true
		}

		// Attempt to permanently delete the dropped database and stop execution if we hit an error
		err = dd.fs.Delete(path, true)
		return err != nil
	}
	iterErr := dd.fs.Iter(droppedDatabaseDirectoryName, false, callback)
	if iterErr != nil {
		return iterErr
	}

	return err
}

// initializeDeletedDatabaseDirectory initializes the special directory Dolt uses to store dropped databases until
// they are fully removed. If the directory is already created and set up correctly, then this method is a no-op.
// If the directory doesn't exist yet, it will be created. If there are any problems initializing the directory, an
// error is returned.
func (dd *droppedDatabaseManager) initializeDeletedDatabaseDirectory() error {
	exists, isDir := dd.fs.Exists(droppedDatabaseDirectoryName)
	if exists && !isDir {
		return fmt.Errorf("%s exists, but is not a directory", droppedDatabaseDirectoryName)
	}

	if exists {
		return nil
	}

	return dd.fs.MkDirs(droppedDatabaseDirectoryName)
}

func (dd *droppedDatabaseManager) ListDroppedDatabases(_ *sql.Context) ([]string, error) {
	if err := dd.initializeDeletedDatabaseDirectory(); err != nil {
		return nil, fmt.Errorf("unable to list undroppable database: %w", err)
	}

	databaseNames := make([]string, 0, 5)
	callback := func(path string, size int64, isDir bool) (stop bool) {
		// When we move a database to the dropped database directory, we normalize the physical directory
		// name to be the same as the logical SQL name, so there's no need to do any name mapping here.
		databaseNames = append(databaseNames, filepath.Base(path))
		return false
	}

	if err := dd.fs.Iter(droppedDatabaseDirectoryName, false, callback); err != nil {
		return nil, err
	}

	return databaseNames, nil
}

// validateUndropDatabase validates that the database |name| is available to be "undropped" and that no existing
// database is already being managed that has the same (case-insensitive) name. If any problems are encountered,
// an error is returned.
func (dd *droppedDatabaseManager) validateUndropDatabase(ctx *sql.Context, name string) (sourcePath, destinationPath, exactCaseName string, err error) {
	availableDatabases, err := dd.ListDroppedDatabases(ctx)
	if err != nil {
		return "", "", "", err
	}

	found, exactCaseName := hasCaseInsensitiveMatch(availableDatabases, name)
	if !found {
		return "", "", "", fmt.Errorf("no database named '%s' found to undrop. %s",
			name, errors.CreateUndropErrorMessage(availableDatabases))
	}

	// Check to see if the destination directory for restoring the database already exists (case-insensitive match)
	destinationPath, err = dd.fs.Abs(exactCaseName)
	if err != nil {
		return "", "", "", err
	}

	if hasCaseInsensitivePath(dd.fs, destinationPath) {
		return "", "", "", fmt.Errorf("unable to undrop database '%s'; "+
			"another database already exists with the same case-insensitive name", exactCaseName)
	}

	sourcePath = filepath.Join(droppedDatabaseDirectoryName, exactCaseName)
	return sourcePath, destinationPath, exactCaseName, nil
}

// hasCaseInsensitivePath returns true if the specified path |target| already exists on the filesystem |fs|, with
// a case-insensitive match on the final component of the path. Note that only the final component of the path is
// checked in a case-insensitive match – the other components of the path must be a case-sensitive match.
func hasCaseInsensitivePath(fs filesys.Filesys, target string) bool {
	found := false
	fs.Iter(filepath.Dir(target), false, func(path string, size int64, isDir bool) (stop bool) {
		if strings.EqualFold(filepath.Base(path), filepath.Base(target)) {
			found = true
		}
		return found
	})
	return found
}

// hasCaseInsensitiveMatch tests to see if any of |candidates| are a case-insensitive match for |target| and if so,
// returns true along with the exact candidate string that matched. If there was not a match, false and the empty
// string are returned.
func hasCaseInsensitiveMatch(candidates []string, target string) (bool, string) {
	found := false
	exactCaseName := ""
	for _, s := range candidates {
		if strings.EqualFold(target, s) {
			exactCaseName = s
			found = true
			break
		}
	}

	return found, exactCaseName
}

// prepareToMoveDroppedDatabase checks the specified |targetPath| to make sure there is not already a dropped database
// there, and if so, the existing dropped database will be renamed with a unique suffix. If any problems are encountered,
// such as not being able to rename an existing dropped database, this function will return an error.
func (dd *droppedDatabaseManager) prepareToMoveDroppedDatabase(_ *sql.Context, targetPath string) error {
	if exists, _ := dd.fs.Exists(targetPath); !exists {
		// If there's nothing at the desired targetPath, we're all set
		return nil
	}

	// If there is something already there, pick a new path to move it to
	newPath := fmt.Sprintf("%s.backup.%d", targetPath, time.Now().UnixMilli())
	if exists, _ := dd.fs.Exists(newPath); exists {
		return fmt.Errorf("unable to move existing dropped database out of the way: "+
			"tried to move it to %s", newPath)
	}
	if err := dd.fs.MoveDir(targetPath, newPath); err != nil {
		return fmt.Errorf("unable to move existing dropped database out of the way: %w", err)
	}

	return nil
}
