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

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	WorkingSetName      = "WorkingSet"
	WorkingSetMetaField = "meta"
	WorkingRootRefField = "workingRootRef"
	StagedRootRefField  = "stagedRootRef"
	MergeStateField     = "mergeState"
)

const (
	MergeStateName                 = "MergeState"
	MergeStateCommitField          = "commit"
	MergeStateWorkingPreMergeField = "workingPreMerge"
)

const (
	WorkingSetMetaName             = "WorkingSetMeta"
	WorkingSetMetaNameField        = "name"
	WorkingSetMetaEmailField       = "email"
	WorkingSetMetaTimestampField   = "timestamp"
	WorkingSetMetaDescriptionField = "description"
	WorkingSetMetaVersionField     = "version"
)

const workingSetMetaVersion = "1.0"

type WorkingSetMeta struct {
	Meta types.Struct
}

var mergeStateTemplate = types.MakeStructTemplate(MergeStateName, []string{MergeStateCommitField, MergeStateWorkingPreMergeField})

type WorkingSetSpec struct {
	Meta        WorkingSetMeta
	WorkingRoot types.Ref
	StagedRoot  types.Ref
	MergeState  *types.Ref
}

// NewWorkingSet creates a new working set object.
// A working set is a value that has been persisted but is not necessarily referenced by a Commit. As the name implies,
// it's storage for data changes that have not yet been incorporated into the commit graph but need durable storage.
//
// A working set struct has the following type:
//
// ```
// struct WorkingSet {
//   meta: M,
//   workingRootRef: R,
//   stagedRootRef: R,
//   mergeState: R,
// }
// ```
// where M is a struct type and R is a ref type.
func NewWorkingSet(_ context.Context, meta WorkingSetMeta, workingRef, stagedRef types.Ref, mergeStateRef *types.Ref) (types.Struct, error) {
	fields := make(types.StructData)
	fields[WorkingSetMetaField] = meta.Meta
	fields[WorkingRootRefField] = workingRef
	fields[StagedRootRefField] = stagedRef

	if mergeStateRef != nil {
		fields[MergeStateField] = mergeStateRef
	}

	return types.NewStruct(workingRef.Format(), WorkingSetName, fields)
}

func NewMergeState(_ context.Context, preMergeWorking types.Ref, commit types.Struct) (types.Struct, error) {
	return mergeStateTemplate.NewStruct(preMergeWorking.Format(), []types.Value{commit, preMergeWorking})
}

func NewWorkingSetMeta(format *types.NomsBinFormat, name, email string, timestamp uint64, description string) (types.Struct, error) {
	fields := make(types.StructData)
	fields[WorkingSetMetaNameField] = types.String(name)
	fields[WorkingSetMetaEmailField] = types.String(email)
	fields[WorkingSetMetaTimestampField] = types.Uint(timestamp)
	fields[WorkingSetMetaDescriptionField] = types.String(description)
	fields[WorkingSetMetaVersionField] = types.String(workingSetMetaVersion)

	return types.NewStruct(format, WorkingSetMetaName, fields)
}

func IsWorkingSet(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		// We're being more lenient here than in other checks, to make it more likely we can release changes to the
		// working set data description in a backwards compatible way.
		// types.IsValueSubtypeOf is very strict about the type description.
		return s.Name() == WorkingSetName, nil
	}
}
