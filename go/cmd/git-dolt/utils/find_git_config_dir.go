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

package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FindGitConfigUnderRoot will recurse upwards from the current working directory
// to the system root looking for a directory named .git, returning its path if found,
// and an error if not.
func FindGitConfigUnderRoot() (string, error) {
	currentPath, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("error getting current directory: %v", err)
	}

	rootPath, err := filepath.Abs("/")
	if err != nil {
		return "", fmt.Errorf("error getting root directory: %v", err)
	}

	return FindGitConfigDir(currentPath, rootPath)
}

// FindGitConfigDir will recurse upwards from currentPath to terminalPath looking for
// a directory named .git, returning its path if found, and an error if not.
//
// Both currentPath and terminalPath are assumed to be absolute paths. An error is returned
// if currentPath is not a descendant of terminalPath.
func FindGitConfigDir(currentPath, terminalPath string) (string, error) {
	if !strings.HasPrefix(currentPath, terminalPath) {
		return "", fmt.Errorf("current path %s is not a descendent of terminal path %s", currentPath, terminalPath)
	}

	// recursive base case -- currentPath and terminalPath are the same
	if currentPath == terminalPath {
		return "", fmt.Errorf("recursed upwards to %s but couldn't find a .git directory", currentPath)
	}

	// check to see if .git exists in currentPath
	fileInfo, fileErr := os.Stat(filepath.Join(currentPath, ".git"))

	// .git exists and is a directory -- success!
	if fileErr == nil && fileInfo.IsDir() {
		return filepath.Join(currentPath, ".git"), nil
	}

	// something went wrong looking for .git other than it not existing -- return an error
	if fileErr != nil && !os.IsNotExist(fileErr) {
		return "", fmt.Errorf("error looking for the .git directory in %s: %v", currentPath, fileErr)
	}

	// either .git exists and is not a directory, or .git does not exist:
	// go up one directory level and make the recursive call
	parentPath := filepath.Dir(currentPath)
	if parentPath == "." {
		return "", fmt.Errorf("ran out of ancestors at %s but couldn't find a .git directory", currentPath)
	}
	return FindGitConfigDir(parentPath, terminalPath)
}
