package doltdb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
)

type stringer string

func (s stringer) String() string {
	return string(s)
}

var hashRegex = regexp.MustCompile(`^[0-9a-v]{32}$`)

const head string = "head"

// IsValidUserBranchName returns true if name isn't a valid commit hash, it is not named "head" and
// it matches the regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidUserBranchName(name string) bool {
	return name != head && !hashRegex.MatchString(name) && ref.IsValidBranchName(name)
}

func IsValidBranchRef(dref ref.DoltRef) bool {
	return dref.GetType() == ref.BranchRefType && IsValidUserBranchName(dref.GetPath())
}

type CommitSpecType string

const (
	RefCommitSpec  CommitSpecType = "ref"
	HashCommitSpec CommitSpecType = "hash"
)

// CommitSpec handles three different types of string representations of commits.  Commits can either be represented
// by the hash of the commit, a branch name, or using "head" to represent the latest commit of the current branch.
// An Ancestor spec can be appended to the end of any of these in order to reach commits that are in the ancestor tree
// of the referenced commit.
type CommitSpec struct {
	CommitStringer fmt.Stringer
	CSType         CommitSpecType
	ASpec          *AncestorSpec
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
		return &CommitSpec{stringer(name), HashCommitSpec, as}, nil
	} else if ref.IsRef(name) {
		dref, err := ref.Parse(name)

		if err != nil {
			return nil, err
		}

		return &CommitSpec{dref, RefCommitSpec, as}, nil
	} else if IsValidUserBranchName(name) {
		return &CommitSpec{ref.NewBranchRef(name), RefCommitSpec, as}, nil
	}

	return nil, ErrInvalidBranchOrHash
}
