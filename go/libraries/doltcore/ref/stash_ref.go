// Copyright 2023 Dolthub, Inc.
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

import (
	"strings"
)

type StashRef struct {
	stash string
}

var _ DoltRef = StashRef{}

// NewStashRef creates a reference to a stashes list.
func NewStashRef(stashName string) StashRef {
	if IsRef(stashName) {
		prefix := PrefixForType(StashRefType)
		if strings.HasPrefix(stashName, prefix) {
			stashName = stashName[len(prefix):]
		} else {
			panic(stashName + " is a ref that is not of type " + prefix)
		}
	}

	return StashRef{stashName}
}

// GetType will return StashRefType
func (br StashRef) GetType() RefType {
	return StashRefType
}

// GetPath returns the name of the tag
func (br StashRef) GetPath() string {
	return br.stash
}

// String returns the fully qualified reference name e.g. refs/heads/main
func (br StashRef) String() string {
	return String(br)
}
