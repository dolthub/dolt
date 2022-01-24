// Copyright 2019-2022 Dolthub, Inc.
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

package dolt_builder

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// GitVersion runs git version
func GitVersion(ctx context.Context) error {
	checkGit := ExecCommand(ctx, "git", "version")
	err := checkGit.Run()
	if err != nil {
		helpStr := "dolt-builder requires git.\n" +
			"Make sure git is installed and on your PATH.\n" +
			"git version: %v\n"
		return fmt.Errorf(helpStr, err)
	}
	return nil
}

// GitClone clones the dolt repo into `${dir}/dolt.git`
func GitCloneBare(ctx context.Context, dir string) error {
	clone := ExecCommand(ctx, "git", "clone", "--bare", GithubDolt)
	clone.Dir = dir
	return clone.Run()
}

func CommitArg(c string) string {
	if IsCommit(c) {
		return c
	}
	if strings.HasPrefix(c, "v") {
		return "tags/" + c
	}
	return "tags/v" + c
}

// GitCheckoutTree checks out `commit` from the Git repo at
// `repoDir` into `toDir`. It does it without copying the entire
// git repository. First we run `git read-tree` with a GIT_INDEX_FILE set to
// `$toDir/.buildindex`, which gets an index for the commit fully populated
// into the file. Then we run `git checkout-index -a` referencing the same
// INDEX_FILE, which populates the current working directory (`toDir`) with the
// contents of the index file.
func GitCheckoutTree(ctx context.Context, repoDir string, toDir string, commit string) error {
	env := os.Environ()
	env = append(env, "GIT_DIR="+repoDir)
	env = append(env, "GIT_INDEX_FILE=.buildindex")
	env = append(env, "GIT_WORK_TREE=.")

	read := ExecCommand(ctx, "git", "read-tree", CommitArg(commit))
	read.Dir = toDir
	read.Env = env
	if err := read.Run(); err != nil {
		return err
	}

	checkout := ExecCommand(ctx, "git", "checkout-index", "-a")
	checkout.Dir = toDir
	checkout.Env = env
	return checkout.Run()
}

// IsCommit returns true if a commit is not a tag
func IsCommit(commit string) bool {
	return strings.IndexByte(commit, '.') == -1
}
