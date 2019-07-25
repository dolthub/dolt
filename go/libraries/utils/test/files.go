// Copyright 2019 Liquidata, Inc.
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

package test

import (
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// TestDir creates a subdirectory inside the systems temp directory
func TestDir(testName string) string {
	id, err := uuid.NewRandom()

	if err != nil {
		panic(ShouldNeverHappen)
	}

	return filepath.Join(os.TempDir(), testName, id.String())
}

// ChangeToTestDir creates a new test directory and changes the current directory to be
func ChangeToTestDir(testName string) (string, error) {
	dir := TestDir(testName)
	err := os.MkdirAll(dir, os.ModePerm)

	if err != nil {
		return "", err
	}

	err = os.Chdir(dir)

	if err != nil {
		return "", err
	}

	return dir, nil
}
