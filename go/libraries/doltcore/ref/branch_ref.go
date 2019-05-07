package ref

import "strings"

type BranchRef struct {
	branch string
}

func (br BranchRef) GetType() RefType {
	return BranchRefType
}

func (br BranchRef) GetPath() string {
	return br.branch
}

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
