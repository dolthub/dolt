package doltdb

import (
	"regexp"
	"strings"
)

var wordRegex = `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+`
var UserBranchRegexStr = "^" + wordRegex + "$"
var RemoteBranchRegexStr = "remotes/" + wordRegex + "/" + wordRegex
var hashRegex = regexp.MustCompile(`^[0-9a-v]{32}$`)
var userBranchRegex = regexp.MustCompile(UserBranchRegexStr)
var remoteBranchRegex = regexp.MustCompile(RemoteBranchRegexStr)

const head string = "head"

// IsValidUserBranchName returns true if name isn't a valid commit hash, it is not named "head" and
// it matches the regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidUserBranchName(name string) bool {
	return !hashRegex.MatchString(name) && userBranchRegex.MatchString(name) && name != head
}

func IsValidRemoteBranchName(name string) bool {
	return remoteBranchRegex.MatchString(name)
}

func LongRemoteBranchName(remote, branch string) string {
	return "remotes/" + remote + "/" + branch
}

type CommitSpecType string

const (
	BranchCommitSpec       CommitSpecType = "branch"
	RemoteBranchCommitSpec CommitSpecType = "remote_branch"
	CommitHashSpec         CommitSpecType = "hash"
)

// CommitSpec handles three different types of string representations of commits.  Commits can either be represented
// by the hash of the commit, a branch name, or using "head" to represent the latest commit of the current branch.
// An Ancestor spec can be appended to the end of any of these in order to reach commits that are in the ancestor tree
// of the referenced commit.
type CommitSpec struct {
	name   string
	csType CommitSpecType
	aSpec  *AncestorSpec
}

// NewCommitSpec takes a spec string and the current working branch.  The current working branch is only relevant when
// using "head" to reference a commit, but if it is not needed it will be ignored.
func NewCommitSpec(cSpecStr, cwb string) (*CommitSpec, error) {
	cSpecStrLwr := strings.TrimSpace(cSpecStr)

	name, as, err := SplitAncestorSpec(cSpecStrLwr)

	if err != nil {
		return nil, err
	}

	if strings.ToLower(name) == head {
		name = cwb
	}

	if hashRegex.MatchString(name) {
		return &CommitSpec{name, CommitHashSpec, as}, nil
	} else if userBranchRegex.MatchString(name) {
		return &CommitSpec{name, BranchCommitSpec, as}, nil
	} else if remoteBranchRegex.MatchString(name) {
		return &CommitSpec{name, RemoteBranchCommitSpec, as}, nil
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

// CSpecType gets the type of the commit spec
func (c *CommitSpec) CSpecType() CommitSpecType {
	return c.csType
}
