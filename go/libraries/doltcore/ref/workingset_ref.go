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
	"fmt"
	"path"
	"strings"
)

type WorkingSetRef struct {
	name string
}

var _ DoltRef = WorkingSetRef{}

// NewWorkingSetRef creates a working set ref from a name or a working set ref e.g. my-workspace, or
// refs/workingSets/my-workspace
func NewWorkingSetRef(path string) WorkingSetRef {
	if IsRef(path) {
		prefix := PrefixForType(WorkingSetRefType)
		if strings.HasPrefix(path, prefix) {
			path = path[len(prefix):]
		} else {
			panic(path + " is a ref that is not of type " + prefix)
		}
	}

	return WorkingSetRef{path}
}

// WorkingSetRefForHead returns a new WorkingSetRef for the head ref given, or an error if the ref given doesn't
// represent a head.
func WorkingSetRefForHead(ref DoltRef) (WorkingSetRef, error) {
	switch ref.GetType() {
	case BranchRefType, WorkspaceRefType:
		return NewWorkingSetRef(path.Join(string(ref.GetType()), ref.GetPath())), nil
	default:
		return WorkingSetRef{}, fmt.Errorf("unsupported type of ref for a working set: %s", ref.GetType())
	}
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

