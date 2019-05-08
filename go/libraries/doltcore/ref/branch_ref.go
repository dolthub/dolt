package ref

import "strings"

// BranchRef is a reference to a branch
type BranchRef struct {
	branch string
}

// GetType will return BranchRefType
func (br BranchRef) GetType() RefType {
	return BranchRefType
}

// GetPath returns the name of the branch
func (br BranchRef) GetPath() string {
	return br.branch
}

// String returns the fully qualified reference name e.g. refs/heads/master
func (br BranchRef) String() string {
	return String(br)
}

func (br BranchRef) MarshalJSON() ([]byte, error) {
	return MarshalJSON(br)
}

// NewBranchRef creates a reference to a local branch from a branch name or a branch ref e.g. master, or refs/heads/master
func NewBranchRef(branchName string) BranchRef {
	if IsRef(branchName) {
		prefix := PrefixForType(BranchRefType)
		if strings.HasPrefix(branchName, prefix) {
			branchName = branchName[len(prefix):]
		} else {
			panic(branchName + " is a ref that is not of type " + prefix)
		}
	}

	return BranchRef{branchName}
}
