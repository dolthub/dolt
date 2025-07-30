// Copyright 2019 Dolthub, Inc.
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

package merge

type TableMergeOp int

const (
	TableUnmodified TableMergeOp = iota
	TableAdded
	TableRemoved
	TableModified
)

type MergeStats struct {
	Operation            TableMergeOp
	Adds                 int
	Deletes              int
	Modifications        int
	DataConflicts        int
	SchemaConflicts      int
	RootObjectConflicts  int
	ConstraintViolations int
}

func (ms *MergeStats) HasArtifacts() bool {
	return ms.HasConflicts() || ms.HasConstraintViolations()
}

func (ms *MergeStats) HasConflicts() bool {
	return ms.HasDataConflicts() || ms.HasSchemaConflicts() || ms.HasRootObjectConflicts()
}

func (ms *MergeStats) HasDataConflicts() bool {
	return ms.DataConflicts > 0
}

func (ms *MergeStats) HasSchemaConflicts() bool {
	return ms.SchemaConflicts > 0
}

func (ms *MergeStats) HasRootObjectConflicts() bool {
	return ms.RootObjectConflicts > 0
}

func (ms *MergeStats) HasConstraintViolations() bool {
	return ms.ConstraintViolations > 0
}
