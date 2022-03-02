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
	Name string
	Email string
	Description string
	Timestamp uint64
}

func (m *WorkingSetMeta) toNomsStruct(format *types.NomsBinFormat) (types.Struct, error) {
	fields := make(types.StructData)
	fields[WorkingSetMetaNameField] = types.String(m.Name)
	fields[WorkingSetMetaEmailField] = types.String(m.Email)
	fields[WorkingSetMetaTimestampField] = types.Uint(m.Timestamp)
	fields[WorkingSetMetaDescriptionField] = types.String(m.Description)
	fields[WorkingSetMetaVersionField] = types.String(workingSetMetaVersion)
	return types.NewStruct(format, WorkingSetMetaName, fields)
}

func WorkingSetMetaFromWorkingSetSt(workingSetSt types.Struct) (*WorkingSetMeta, error) {
	metaV, ok, err := workingSetSt.MaybeGet(WorkingSetMetaNameField)
	if err != nil || !ok {
		return nil, err
	}
	return workingSetMetaFromNomsSt(metaV.(types.Struct))
}

var mergeStateTemplate = types.MakeStructTemplate(MergeStateName, []string{MergeStateCommitField, MergeStateWorkingPreMergeField})

type WorkingSetSpec struct {
	Meta        *WorkingSetMeta
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
func newWorkingSet(_ context.Context, meta *WorkingSetMeta, workingRef, stagedRef types.Ref, mergeStateRef *types.Ref) (types.Struct, error) {
	metaSt, err := meta.toNomsStruct(workingRef.Format())
	if err != nil {
		return types.Struct{}, err
	}

	fields := make(types.StructData)
	fields[WorkingSetMetaField] = metaSt
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

func workingSetMetaFromNomsSt(st types.Struct) (*WorkingSetMeta, error) {
	// Like other places that deal with working set meta, we err on the side of leniency w.r.t. this data structure's
	// contents
	name, ok, err := st.MaybeGet(WorkingSetMetaNameField)
	if err != nil {
		return nil, err
	}
	if !ok {
		name = types.String("not present")
	}

	email, ok, err := st.MaybeGet(WorkingSetMetaEmailField)
	if err != nil {
		return nil, err
	}
	if !ok {
		email = types.String("not present")
	}

	timestamp, ok, err := st.MaybeGet(WorkingSetMetaTimestampField)
	if err != nil {
		return nil, err
	}
	if !ok {
		timestamp = types.Uint(0)
	}

	description, ok, err := st.MaybeGet(WorkingSetMetaDescriptionField)
	if err != nil {
		return nil, err
	}
	if !ok {
		description = types.String("not present")
	}

	return &WorkingSetMeta{
		Name:        string(name.(types.String)),
		Email:       string(email.(types.String)),
		Timestamp:   uint64(timestamp.(types.Uint)),
		Description: string(description.(types.String)),
	}, nil
}
