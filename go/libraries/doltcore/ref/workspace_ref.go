// Copyright 2020 Dolthub, Inc.
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

type WorkspaceRef struct {
	workspace string
}

var _ DoltRef = WorkspaceRef{}

// NewWorkspaceRef creates a reference to a local workspace from a workspace name or a workspace ref e.g. v1, or refs/workspace/v1
func NewWorkspaceRef(workspace string) WorkspaceRef {
	if IsRef(workspace) {
		prefix := PrefixForType(WorkspaceRefType)
		if strings.HasPrefix(workspace, prefix) {
			workspace = workspace[len(prefix):]
		} else {
			panic(workspace + " is a ref that is not of type " + prefix)
		}
	}

	return WorkspaceRef{workspace}
}

// GetType will return WorkspaceRefType
func (br WorkspaceRef) GetType() RefType {
	return WorkspaceRefType
}

// GetPath returns the name of the workspace
func (br WorkspaceRef) GetPath() string {
	return br.workspace
}

// String returns the fully qualified reference name e.g. refs/workspace/v1
func (br WorkspaceRef) String() string {
	return String(br)
}

// MarshalJSON serializes a WorkspaceRef to JSON.
func (br WorkspaceRef) MarshalJSON() ([]byte, error) {
	return MarshalJSON(br)
}
