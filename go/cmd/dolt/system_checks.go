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

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// reconfigIfTempFileMoveFails checks to see if the file system used for the data directory supports moves from TMPDIR.
// If this is not possible, we can't perform atomic moves of storage files, so we force the temp dir to be in the datadir
// to assure they are on the same file system.
func reconfigIfTempFileMoveFails(dataDir filesys.Filesys) error {
	absP, err := dataDir.Abs("")
	if err != nil {
		return err
	}

	dotDoltCreated := false
	tmpDirCreated := false

	doltDir := filepath.Join(absP, dbfactory.DoltDir)
	stat, err := os.Stat(doltDir)
	if err != nil {
		err := os.MkdirAll(doltDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create dolt dir '%s': %s", doltDir, err.Error())
		}

		dotDoltCreated = true
	}

	doltTmpDir := filepath.Join(doltDir, env.TmpDirName)
	stat, err = os.Stat(doltTmpDir)
	if err != nil {
		err := os.MkdirAll(doltTmpDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create temp dir '%s': %s", doltTmpDir, err.Error())
		}
		tmpDirCreated = true

	} else if !stat.IsDir() {
		return fmt.Errorf("attempting to use '%s' as a temp directory, but there exists a file with that name", doltTmpDir)
	}

	tmpF, err := os.CreateTemp("", "")
	if err != nil {
		return err
	}

	name := tmpF.Name()
	err = tmpF.Close()
	if err != nil {
		return err
	}

	movedName := filepath.Join(doltTmpDir, "testfile")

	if os.Getenv("DOLT_FORCE_LOCAL_TEMP_FILES") == "" {
		err = file.Rename(name, movedName)
	} else {
		err = errors.New("treating rename as failed because DOLT_FORCE_LOCAL_TEMP_FILES is set")
	}
	if err == nil {
		// If rename was successful, then the tmp dir is fine, so no need to change it. Clean up the things we created.
		_ = file.Remove(movedName)

		if tmpDirCreated {
			_ = file.Remove(doltTmpDir)
		}

		if dotDoltCreated {
			_ = file.Remove(doltDir)
		}

		return nil
	}
	_ = file.Remove(name)

	// Rename failed. So we force the tmp dir to be the data dir.
	tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(doltTmpDir)

	return nil
}
