package doltdb

import (
	"regexp"
	"strings"
)

var UserBranchRegexStr = `^[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
var hashRegex, _ = regexp.Compile(`^[0-9a-v]{32}$`)
var userBranchRegex, _ = regexp.Compile(UserBranchRegexStr)

// IsValidUserBranch returns true if name isn't a valid commit hash, it is not named "head" and it matches the
// regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidUserBranchName(name string) bool {
	return !hashRegex.MatchString(name) && userBranchRegex.MatchString(name) && name != "head"
}

type commitSpecType string

const (
	branchCommitSpec commitSpecType = "branch"
	commitHashSpec   commitSpecType = "hash"
)

// CommitSpec handles three different types of string representations of commits.  Commits can either be represented
// by the hash of the commit, a branch name, or using "head" to represent the lastest commit of the current branch.
// An Ancestor spec can be appendend to the end of any of these in order to reach commits that are in the ancestor tree
// of the referenced commit.
type CommitSpec struct {
	name   string
	csType commitSpecType
	aSpec  *AncestorSpec
}

// NewCommitSpec takes a spec string and the current working branch.  The current working branch is only relavent when
// using "head" to reference a commbit, but if it is not needed it will be ignored.
func NewCommitSpec(cSpecStr, cwb string) (*CommitSpec, error) {
	cwbLwr := strings.ToLower(strings.TrimSpace(cwb))
	cSpecStrLwr := strings.ToLower(strings.TrimSpace(cSpecStr))

	name, as, err := SplitAncestorSpec(cSpecStrLwr)

	if err != nil {
		return nil, err
	}

	if name == "head" {
		name = cwbLwr
	}

	if hashRegex.MatchString(name) {
		return &CommitSpec{name, commitHashSpec, as}, nil
	} else if userBranchRegex.MatchString(name) {
		return &CommitSpec{name, branchCommitSpec, as}, nil
	} else {
		return nil, ErrInvalidBranchOrHash
	}
}

// Name gets the name of the commit.  Will either be a branch name, or a commit hash
func (c *CommitSpec) Name() string {
	return c.name
}

// AncestorSpec gets the ancestor spec string
func (c *CommitSpec) AncestorSpec() *AncestorSpec {
	return c.aSpec
}

//
