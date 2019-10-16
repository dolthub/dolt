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
	"Dustin Brown <dustin@liquidata.co>":   nil,
	"GitHub <noreply@github.com>":          nil,
	"Katie McCulloch <katie@liquidata.co>": nil,
	"Matt Jesuele <matt@liquidata.co>":     nil,
	"Oscar Batori <oscar@liquidata.co>":    nil,
	"Osheiza Otori <osheiza@liquidata.co>": nil,
	"Taylor Bantle <taylor@liquidata.co>":  nil,
	"Timothy Sehn <tim@liquidata.co>":      nil,
	"Tim Sehn <tim@liquidata.co>":          nil,
	"Zach Musgrave <zach@liquidata.co>":    nil,
}

var AllowedCommitters = map[string]*struct{}{
	"Aaron Son <aaron@liquidata.co>":       nil,
	"Brian Hendriks <brian@liquidata.co>":  nil,
	"Daylon Wilkins <daylon@liquidata.co>": nil,
	"Dustin Brown <dustin@liquidata.co>":   nil,
	"GitHub <noreply@github.com>":          nil,
	"Katie McCulloch <katie@liquidata.co>": nil,
	"Matt Jesuele <matt@liquidata.co>":     nil,
	"Oscar Batori <oscar@liquidata.co>":    nil,
	"Osheiza Otori <osheiza@liquidata.co>": nil,
	"Taylor Bantle <taylor@liquidata.co>":  nil,
	"Timothy Sehn <tim@liquidata.co>":      nil,
	"Tim Sehn <tim@liquidata.co>":          nil,
	"Zach Musgrave <zach@liquidata.co>":    nil,
}

// Attempt to enforce some rules around what commits in incoming PRs
// look like.
// Rules:
// - Committer for every commit appears in AllowedCommitters.
// - Author for every commit appears in AllowedAuthors.
// - This would be a place to enforce DCO or CLA requirements.

func main() {
	if len(os.Args) < 2 {
		PrintUsageAndExit()
	}
	if os.Args[1] == "-pr" {
		if len(os.Args) != 4 {
			PrintUsageAndExit()
		}
		HandleCheckAndExit(Check("remotes/origin/"+os.Args[2], "remotes/origin/"+os.Args[3]))
	} else if os.Args[1] == "-dir" {
		if len(os.Args) > 3 {
			PrintUsageAndExit()
		}
		target := "master"
		if len(os.Args) == 3 {
			target = os.Args[2]
		}
		HandleCheckAndExit(Check("HEAD", "remotes/origin/"+target))
	} else {
		PrintUsageAndExit()
	}
}

func HandleCheckAndExit(failed bool) {
	if failed {
		fmt.Printf("\nThis PR has non-whitelisted committers or authors.\n")
		fmt.Printf("Please use ./utils/checkcommitters/fix_committer.sh to make\n")
		fmt.Printf("all commits from a whitelisted committer and author.\n")
		os.Exit(1)
	}
}

func PrintUsageAndExit() {
	fmt.Printf("Usage: checkcommitters [-pr SOURCE_BRANCH TARGET_BRANCH | -dir TARGET_BRANCH\n")
	fmt.Printf("  SOURCE_BRANCH is the remotes/origin branch to be merged by the PR, for example PR-4.\n")
	fmt.Printf("  CHANGE_TARGET is the target remotes/origin branch of the PR, for example master.\n")
	fmt.Printf("This should be run from the git checkout workspace for the PR.\n")
	fmt.Printf("Example: checkcommitters -pr PR-4 master.\n")
	fmt.Printf("  Will check that all commits from merge-base of PR-4 and remotes/origin/master HEAD conform.\n")
	fmt.Printf("Example: checkcommitters -dir master\n")
	fmt.Printf("  Will check that all commits from remotes/origin/master..HEAD conform.\n")
	os.Exit(1)
}

func Check(source, target string) bool {
	mbc := exec.Command("git", "merge-base", source, target)
	mbco, err := mbc.CombinedOutput()
	if err != nil {
		log.Fatalf("Error running `git merge-base %s %s` to find merge parent: %v\n", source, target, err)
	}
	base := strings.TrimSpace(string(mbco))

	return CheckFromBase(base, source)
}

func CheckFromBase(base string, source string) bool {
	lc := exec.Command("git", "log", "--format=full", base+".."+source)
	lco, err := lc.CombinedOutput()
	if err != nil {
		log.Fatalf("Error running `git log --format=full %s..%s`: %v\n", base, source, err)
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
	return failed
}
