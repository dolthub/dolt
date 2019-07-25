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

package ref

import "strings"

var EmptyBranchRef = BranchRef{""}

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
