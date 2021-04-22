// Copyright 2021 Dolthub, Inc.
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

type WorkingSetRef struct {
	name string
}

var _ DoltRef = WorkingSetRef{}

// NewWorkingSetRef creates a reference to a working set ref from a name
// or a working set ref e.g. my-workspace, or refs/workingSets/my-workspace
func NewWorkingSetRef(workspace string) WorkingSetRef {
	if IsRef(workspace) {
		prefix := PrefixForType(WorkingSetRefType)
		if strings.HasPrefix(workspace, prefix) {
			workspace = workspace[len(prefix):]
		} else {
			panic(workspace + " is a ref that is not of type " + prefix)
		}
	}

	return WorkingSetRef{workspace}
}

// GetType will return WorkingSetRefType
func (r WorkingSetRef) GetType() RefType {
	return WorkingSetRefType
}

// GetPath returns the name of the working set
func (r WorkingSetRef) GetPath() string {
	return r.name
}

// String returns the fully qualified reference name e.g.
// refs/workingSets/my-branch
func (r WorkingSetRef) String() string {
	return String(r)
}

// MarshalJSON serializes a WorkingSetRef to JSON.
func (r WorkingSetRef) MarshalJSON() ([]byte, error) {
	return MarshalJSON(r)
}

