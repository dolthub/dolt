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

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

var AllowedAuthors = map[string]*struct{}{
	"Aaron Son <aaron@liquidata.co>":       nil,
	"Brian Hendriks <brian@liquidata.co>":  nil,
	"Daylon Wilkins <daylon@liquidata.co>": nil,
	"Katie McCulloch <katie@liquidata.co>": nil,
	"Matt Jesuele <matt@liquidata.co>":     nil,
	"Osheiza Otori <osheiza@liquidata.co>": nil,
	"Timothy Sehn <tim@liquidata.co>":      nil,
	"Zach Musgrave <zach@liquidata.co>":    nil,
}

var AllowedCommitters = map[string]*struct{}{
	"Aaron Son <aaron@liquidata.co>":       nil,
	"Brian Hendriks <brian@liquidata.co>":  nil,
	"Daylon Wilkins <daylon@liquidata.co>": nil,
	"Katie McCulloch <katie@liquidata.co>": nil,
	"Matt Jesuele <matt@liquidata.co>":     nil,
	"Osheiza Otori <osheiza@liquidata.co>": nil,
	"Timothy Sehn <tim@liquidata.co>":      nil,
	"Zach Musgrave <zach@liquidata.co>":    nil,
}

// Attempt to enforce some rules around what commits in incoming PRs
// look like.
// Rules:
// - Committer for every commit appears in AllowedCommitters.
// - Author for every commit appears in AllowedAuthors.
// - This would be a place to enforce DCO or CLA requirements.

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: checkcommitters GIT_COMMIT CHANGE_TARGET\n")
		fmt.Printf("  GIT_COMMIT is the commit to be merged by the PR.\n")
		fmt.Printf("  CHANGE_TARGET is the target branch, for example master.\n")
		fmt.Printf("This should be run from the git checkout workspace for the PR.\n")
		os.Exit(1)
	}
	mbc := exec.Command("git", "merge-base", "remotes/origin/"+os.Args[2], os.Args[1])
	mbco, err := mbc.CombinedOutput()
	if err != nil {
		log.Fatalf("Error running `git merge-base remotes/origin/%s %s` to find merge parent: %v\n", os.Args[2], os.Args[1], err)
	}
	base := strings.TrimSpace(string(mbco))
	lc := exec.Command("git", "log", "--format=full", base+".."+os.Args[1])
	lco, err := lc.CombinedOutput()
	if err != nil {
		log.Fatalf("Error running `git log --format=full %s..%s`: %v\n", base, os.Args[1], err)
	}

	var failed bool
	var commit string
	for {
		n := bytes.IndexByte(lco, byte('\n'))
		if n == -1 {
			break
		}
		if bytes.HasPrefix(lco, []byte("commit ")) {
			commit = string(lco[7:47])
		} else if bytes.HasPrefix(lco, []byte("Author: ")) {
			author := string(lco[8:n])
			if _, ok := AllowedAuthors[author]; !ok {
				fmt.Printf("Unallowed Author: value on commit %s: %s\n", commit, author)
				failed = true
			}
		} else if bytes.HasPrefix(lco, []byte("Commit: ")) {
			committer := string(lco[8:n])
			if _, ok := AllowedCommitters[committer]; !ok {
				fmt.Printf("Unallowed Commit: value on commit %s: %s\n", commit, committer)
				failed = true
			}
		}
		lco = lco[n+1:]
	}
	if failed {
		fmt.Printf("\n\nThis PR has non-whitelisted committers or authors.\n")
		fmt.Printf("Please use git rebase or git filter-branch to ensure every commit\n")
		fmt.Printf("is from a whitelisted committer and author.\n")
		os.Exit(1)
	}
}
