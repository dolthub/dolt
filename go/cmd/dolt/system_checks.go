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
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// reconfigIfTempFileMoveFails checks to see if the file system used for the data directory supports moved from TMPDIR.
// If this is not possible, we can't perform automic moves of storage files, so we force the temp dir to be the data dir
// to assure they are on the same file system.
func reconfigIfTempFileMoveFails(dataDir filesys.Filesys) error {
	absP, err := dataDir.Abs("")
	if err != nil {
		return err
	}

	dotDoltCreated := false
	tmpDirCreated := false

	doltDir := filepath.Join(absP, ".dolt")
	stat, err := os.Stat(doltDir)
	if err != nil {
		err := os.MkdirAll(doltDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create dolt dir '%s': %s", doltDir, err.Error())
		}

		dotDoltCreated = true
	}

	doltTmpDir := filepath.Join(doltDir, "tmp")
	stat, err = os.Stat(doltTmpDir)
	if err != nil {
		err := os.MkdirAll(doltTmpDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create temp dir '%s': %s", doltTmpDir, err.Error())
		}
		tmpDirCreated = true

	} else if !stat.IsDir() {
		// Should the file exist?? If it does is it a problem? Is it a directory??
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

	err = file.Rename(name, movedName)
	if err == nil {
		// tmp file system is the same as the data dir, so no need to change it.
		_ = file.Remove(movedName)

		if tmpDirCreated {
			_ = file.Remove(doltTmpDir)
		}

		if dotDoltCreated {
			_ = file.Remove(doltDir)
		}

		return nil
	}
	_ = file.Remove(movedName)

	// Rename failed. So we force the tmp dir to be the data dir.
	tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(doltTmpDir)

	return nil
}
