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
	"io/ioutil"
	"os"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

// returns false if it fails to verify that it can move files from the default temp directory to the local directory.
func canMoveTempFile() bool {
	const testfile = "./testfile"

	f, err := ioutil.TempFile("", "")

	if err != nil {
		return false
	}

	name := f.Name()
	err = f.Close()

	if err != nil {
		return false
	}

	err = file.Rename(name, testfile)

	if err != nil {
		_ = file.Remove(name)
		return false
	}

	_ = file.Remove(testfile)
	return true
}

// If we cannot verify that we can move files for any reason, use a ./.dolt/tmp as the temp dir.
func reconfigIfTempFileMoveFails(dEnv *env.DoltEnv) error {
	if !canMoveTempFile() {
		tmpDir := "./.dolt/tmp"

		if !dEnv.HasDoltDir() {
			tmpDir = "./.tmp"
		}

		stat, err := os.Stat(tmpDir)

		if err != nil {
			err := os.MkdirAll(tmpDir, os.ModePerm)

			if err != nil {
				return fmt.Errorf("failed to create temp dir '%s': %s", tmpDir, err.Error())
			}
		} else if !stat.IsDir() {
			return fmt.Errorf("attempting to use '%s' as a temp directory, but there exists a file with that name", tmpDir)
		}

		tempfiles.MovableTempFileProvider = tempfiles.NewTempFileProviderAt(tmpDir)
	}

	return nil
}
