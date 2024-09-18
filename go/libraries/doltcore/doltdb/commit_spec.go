// Copyright 2019 Dolthub, Inc.
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

package doltdb

import (
	"regexp"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

var hashRegex = regexp.MustCompile(`^[0-9a-v]{32}$`)

const head string = "head"

// IsValidUserBranchName returns true if name isn't a valid commit hash, it is not named "head" and
// it matches the regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidUserBranchName(name string) bool {
	return name != head && !hashRegex.MatchString(name) && ref.IsValidBranchName(name)
}

// IsValidBranchRef validates that a BranchRef doesn't violate naming constraints.
func IsValidBranchRef(dref ref.DoltRef) bool {
	return dref.GetType() == ref.BranchRefType && IsValidUserBranchName(dref.GetPath())
}

// IsValidTagRef validates that a TagRef doesn't violate naming constraints.
func IsValidTagRef(dref ref.DoltRef) bool {
	s := dref.GetPath()
	return dref.GetType() == ref.TagRefType &&
		s != head &&
		!hashRegex.MatchString(s) &&
		ref.IsValidTagName(s)
}

func IsValidCommitHash(s string) bool {
	return hashRegex.MatchString(s)
}

type commitSpecType string

const (
	refCommitSpec  commitSpecType = "ref"
	hashCommitSpec commitSpecType = "hash"
	headCommitSpec commitSpecType = "head"
)

// CommitSpec handles three different types of string representations of commits.  Commits can either be represented
// by the hash of the commit, a branch name, or using "head" to represent the latest commit of the current branch.
// An Ancestor spec can be appended to the end of any of these in order to reach commits that are in the ancestor tree
// of the referenced commit.
type CommitSpec struct {
	baseSpec string
	csType   commitSpecType
	aSpec    *AncestorSpec
}

// NewCommitSpec parses a string specifying a commit using dolt commit spec
// syntax and returns a |*CommitSpec|. A commit spec has a base commit and an
// optional ancestor specification. The syntax admits three types of base
// commit references:
// * head -- the literal string HEAD specifies the HEAD reference of the
// current working set.
// * a commit hash, like 46m0aqr8c1vuv76ml33cdtr8722hsbhn -- a fully specified
// commit hash.
// * a ref -- referring to a branch or tag reference in the current dolt database.
// Examples of branch refs include `master`, `heads/master`, `refs/heads/master`,
// `origin/master`, `refs/remotes/origin/master`.
// Examples of tag refs include `v1.0`, `tags/v1.0`, `refs/tags/v1.0`,
// `origin/v1.0`, `refs/remotes/origin/v1.0`.
//
// A commit spec has an optional ancestor specification, which describes a
// traversal of commit parents, starting at the base commit, in order to arrive
// at the actually specified commit. See |AncestorSpec|. Examples of
// |CommitSpec|s:
// * HEAD
// * master
// * HEAD~
// * remotes/origin/master~~
// * refs/heads/my-feature-branch^2~
//
// Constructing a |CommitSpec| does not mean the specified branch or commit
// exists. This carries a description of how to find the specified commit. See
// |doltdb.Resolve| for resolving a |CommitSpec| to a |Commit|.
func NewCommitSpec(cSpecStr string) (*CommitSpec, error) {
	cSpecStrLwr := strings.TrimSpace(cSpecStr)

	name, as, err := SplitAncestorSpec(cSpecStrLwr)
	if err != nil {
		return nil, err
	}

	if strings.EqualFold(name, head) {
		return &CommitSpec{head, headCommitSpec, as}, nil
	}
	if hashRegex.MatchString(name) {
		return &CommitSpec{name, hashCommitSpec, as}, nil
	}
	if !ref.IsValidBranchName(name) {
		return nil, ErrInvalidBranchOrHash
	}
	return &CommitSpec{name, refCommitSpec, as}, nil
}
