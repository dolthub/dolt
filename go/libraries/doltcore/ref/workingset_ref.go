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
	"errors"
	"fmt"
	"path"
	"strings"
)

// A WorkingSetRef is not a DoltRef, and doesn't live in the |refs/| namespace. But it functions similarly to DoltRefs
type WorkingSetRef struct {
	name string
}

const WorkingSetRefPrefix = "workingSets"

// NewWorkingSetRef creates a working set ref from a name or a working set ref e.g. heads/main, or
// workingSets/heads/main
func NewWorkingSetRef(workingSetName string) WorkingSetRef {
	prefix := WorkingSetRefPrefix + "/"
	if strings.HasPrefix(workingSetName, prefix) {
		workingSetName = workingSetName[len(prefix):]
	}

	return WorkingSetRef{workingSetName}
}

var ErrWorkingSetUnsupported = errors.New("unsupported type of ref for a working set")

// WorkingSetRefForHead returns a new WorkingSetRef for the head ref given, or an error if the ref given doesn't
// represent a head.
func WorkingSetRefForHead(ref DoltRef) (WorkingSetRef, error) {
	switch ref.GetType() {
	case BranchRefType, WorkspaceRefType:
		return NewWorkingSetRef(path.Join(string(ref.GetType()), ref.GetPath())), nil
	case RemoteRefType:
		rmtRef, ok := ref.(RemoteRef)
		if !ok {
			return WorkingSetRef{}, fmt.Errorf("%w: %s", ErrWorkingSetUnsupported, ref.GetType())
		}
		// NM4 Not sure if that's right AT ALL. Modifying a remote workingSet is apparently something
		// we've never done before (?)
		return NewWorkingSetRef(path.Join(string(BranchRefType), rmtRef.GetBranch())), nil

	default:
		return WorkingSetRef{}, fmt.Errorf("%w: %s", ErrWorkingSetUnsupported, ref.GetType())
	}
}

// GetPath returns the name of the working set
func (r WorkingSetRef) GetPath() string {
	return r.name
}

func (r WorkingSetRef) ToHeadRef() (DoltRef, error) {
	return Parse(refPrefix + r.GetPath())
}

// String returns the fully qualified reference name e.g.
// refs/workingSets/my-branch
func (r WorkingSetRef) String() string {
	return path.Join(WorkingSetRefPrefix, r.name)
}

// IsWorkingSet returns whether the given ref is a working set
func IsWorkingSet(ref string) bool {
	return strings.HasPrefix(ref, WorkingSetRefPrefix)
}
