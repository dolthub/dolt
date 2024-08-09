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

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	workingSetName      = "WorkingSet"
	workingSetMetaField = "meta"
	workingRootRefField = "workingRootRef"
	stagedRootRefField  = "stagedRootRef"
)

const (
	mergeStateName                 = "MergeState"
	mergeStateField                = "mergeState"
	mergeStateCommitSpecField      = "commitSpec"
	mergeStateCommitField          = "commit"
	mergeStateWorkingPreMergeField = "workingPreMerge"
)

const (
	rebaseStateField = "rebaseState"
)

const (
	workingSetMetaName             = "WorkingSetMeta"
	workingSetMetaNameField        = "name"
	workingSetMetaEmailField       = "email"
	workingSetMetaTimestampField   = "timestamp"
	workingSetMetaDescriptionField = "description"
	workingSetMetaVersionField     = "version"
)

const workingSetMetaVersion = "1.0"

type WorkingSetMeta struct {
	Name        string
	Email       string
	Description string
	Timestamp   uint64
}

func (m *WorkingSetMeta) toNomsStruct(format *types.NomsBinFormat) (types.Struct, error) {
	fields := make(types.StructData)
	fields[workingSetMetaNameField] = types.String(m.Name)
	fields[workingSetMetaEmailField] = types.String(m.Email)
	fields[workingSetMetaTimestampField] = types.Uint(m.Timestamp)
	fields[workingSetMetaDescriptionField] = types.String(m.Description)
	fields[workingSetMetaVersionField] = types.String(workingSetMetaVersion)
	return types.NewStruct(format, workingSetMetaName, fields)
}

func workingSetMetaFromWorkingSetSt(workingSetSt types.Struct) (*WorkingSetMeta, error) {
	metaV, ok, err := workingSetSt.MaybeGet(workingSetMetaNameField)
	if err != nil || !ok {
		return nil, err
	}
	return workingSetMetaFromNomsSt(metaV.(types.Struct))
}

var mergeStateTemplate = types.MakeStructTemplate(mergeStateName, []string{mergeStateCommitField, mergeStateCommitSpecField, mergeStateWorkingPreMergeField})

type WorkingSetSpec struct {
	Meta        *WorkingSetMeta
	WorkingRoot types.Ref
	StagedRoot  types.Ref
	MergeState  *MergeState
	RebaseState *RebaseState
}

// newWorkingSet creates a new working set object.
// A working set is a value that has been persisted but is not necessarily referenced by a Commit. As the name implies,
// it's storage for data changes that have not yet been incorporated into the commit graph but need durable storage.
//
// A working set struct has the following type:
//
// ```
//
//	struct WorkingSet {
//	  meta: M,
//	  workingRootRef: R,
//	  stagedRootRef: R,
//	  mergeState: R,
//	}
//
// ```
// where M is a struct type and R is a ref type.
func newWorkingSet(ctx context.Context, db *database, workingSetSpec WorkingSetSpec) (hash.Hash, types.Ref, error) {
	meta := workingSetSpec.Meta
	workingRef := workingSetSpec.WorkingRoot
	stagedRef := workingSetSpec.StagedRoot
	mergeState := workingSetSpec.MergeState
	rebaseState := workingSetSpec.RebaseState

	if db.Format().UsesFlatbuffers() {
		stagedAddr := stagedRef.TargetHash()
		data := workingset_flatbuffer(workingRef.TargetHash(), &stagedAddr, mergeState, rebaseState, meta)

		r, err := db.WriteValue(ctx, types.SerialMessage(data))
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		ref, err := types.ToRefOfValue(r, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		return ref.TargetHash(), ref, nil
	}

	metaSt, err := meta.toNomsStruct(workingRef.Format())
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}

	fields := make(types.StructData)
	fields[workingSetMetaField] = metaSt
	fields[workingRootRefField] = workingRef
	fields[stagedRootRefField] = stagedRef

	if mergeState != nil {
		fields[mergeStateField] = *mergeState.nomsMergeStateRef
	}

	st, err := types.NewStruct(workingRef.Format(), workingSetName, fields)
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}

	wsRef, err := db.WriteValue(ctx, st)
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}

	ref, err := types.ToRefOfValue(wsRef, db.Format())
	if err != nil {
		return hash.Hash{}, types.Ref{}, err
	}

	return ref.TargetHash(), ref, nil
}

// workingset_flatbuffer creates a flatbuffer message for working set metadata.
func workingset_flatbuffer(working hash.Hash, staged *hash.Hash, mergeState *MergeState, rebaseState *RebaseState, meta *WorkingSetMeta) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	workingoff := builder.CreateByteVector(working[:])
	var stagedOff, mergeStateOff, rebaseStateOffset flatbuffers.UOffsetT
	if staged != nil {
		stagedOff = builder.CreateByteVector((*staged)[:])
	}
	if mergeState != nil {
		prerootaddroff := builder.CreateByteVector((*mergeState.preMergeWorkingAddr)[:])
		fromaddroff := builder.CreateByteVector((*mergeState.fromCommitAddr)[:])
		fromspecoff := builder.CreateString(mergeState.fromCommitSpec)
		unmergableoff := SerializeStringVector(builder, mergeState.unmergableTables)
		serial.MergeStateStart(builder)
		serial.MergeStateAddPreWorkingRootAddr(builder, prerootaddroff)
		serial.MergeStateAddFromCommitAddr(builder, fromaddroff)
		serial.MergeStateAddFromCommitSpecStr(builder, fromspecoff)
		serial.MergeStateAddUnmergableTables(builder, unmergableoff)
		serial.MergeStateAddIsCherryPick(builder, mergeState.isCherryPick)
		mergeStateOff = serial.MergeStateEnd(builder)
	}

	if rebaseState != nil {
		preRebaseRootAddrOffset := builder.CreateByteVector((*rebaseState.preRebaseWorkingAddr)[:])
		ontoAddrOffset := builder.CreateByteVector((*rebaseState.ontoCommitAddr)[:])
		branchOffset := builder.CreateString(rebaseState.branch)
		serial.RebaseStateStart(builder)
		serial.RebaseStateAddPreWorkingRootAddr(builder, preRebaseRootAddrOffset)
		serial.RebaseStateAddBranch(builder, branchOffset)
		serial.RebaseStateAddOntoCommitAddr(builder, ontoAddrOffset)
		serial.RebaseStateAddCommitBecomesEmptyHandling(builder, rebaseState.commitBecomesEmptyHandling)
		serial.RebaseStateAddEmptyCommitHandling(builder, rebaseState.emptyCommitHandling)
		rebaseStateOffset = serial.RebaseStateEnd(builder)
	}

	var nameOff, emailOff, descOff flatbuffers.UOffsetT
	if meta != nil {
		nameOff = builder.CreateString(meta.Name)
		emailOff = builder.CreateString(meta.Email)
		descOff = builder.CreateString(meta.Description)
	}

	serial.WorkingSetStart(builder)
	serial.WorkingSetAddWorkingRootAddr(builder, workingoff)
	if stagedOff != 0 {
		serial.WorkingSetAddStagedRootAddr(builder, stagedOff)
	}
	if mergeStateOff != 0 {
		serial.WorkingSetAddMergeState(builder, mergeStateOff)
	}
	if rebaseStateOffset != 0 {
		serial.WorkingSetAddRebaseState(builder, rebaseStateOffset)
	}

	if meta != nil {
		serial.WorkingSetAddName(builder, nameOff)
		serial.WorkingSetAddEmail(builder, emailOff)
		serial.WorkingSetAddDesc(builder, descOff)
		serial.WorkingSetAddTimestampMillis(builder, meta.Timestamp)
	}
	return serial.FinishMessage(builder, serial.WorkingSetEnd(builder), []byte(serial.WorkingSetFileID))
}

func NewMergeState(
	ctx context.Context,
	vrw types.ValueReadWriter,
	preMergeWorking types.Ref,
	commit *Commit,
	commitSpecStr string,
	unmergableTables []string,
	isCherryPick bool,
) (*MergeState, error) {
	if vrw.Format().UsesFlatbuffers() {
		ms := &MergeState{
			preMergeWorkingAddr: new(hash.Hash),
			fromCommitAddr:      new(hash.Hash),
			fromCommitSpec:      commitSpecStr,
			unmergableTables:    unmergableTables,
			isCherryPick:        isCherryPick,
		}
		*ms.preMergeWorkingAddr = preMergeWorking.TargetHash()
		*ms.fromCommitAddr = commit.Addr()
		return ms, nil
	} else {
		v, err := mergeStateTemplate.NewStruct(preMergeWorking.Format(), []types.Value{commit.NomsValue(), types.String(commitSpecStr), preMergeWorking})
		if err != nil {
			return nil, err
		}
		ref, err := vrw.WriteValue(ctx, v)
		if err != nil {
			return nil, err
		}
		return &MergeState{
			isCherryPick:      isCherryPick,
			nomsMergeStateRef: &ref,
			nomsMergeState:    &v,
		}, nil
	}
}

func NewRebaseState(preRebaseWorkingRoot hash.Hash, commitAddr hash.Hash, branch string, commitBecomesEmptyHandling uint8, emptyCommitHandling uint8) *RebaseState {
	return &RebaseState{
		preRebaseWorkingAddr:       &preRebaseWorkingRoot,
		ontoCommitAddr:             &commitAddr,
		branch:                     branch,
		commitBecomesEmptyHandling: commitBecomesEmptyHandling,
		emptyCommitHandling:        emptyCommitHandling,
	}
}

func IsWorkingSet(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); ok {
		// We're being more lenient here than in other checks, to make it more likely we can release changes to the
		// working set data description in a backwards compatible way.
		// types.IsValueSubtypeOf is very strict about the type description.
		return s.Name() == workingSetName, nil
	} else if sm, ok := v.(types.SerialMessage); ok {
		return serial.GetFileID(sm) == serial.WorkingSetFileID, nil
	} else {
		return false, nil
	}
}

func workingSetMetaFromNomsSt(st types.Struct) (*WorkingSetMeta, error) {
	// Like other places that deal with working set meta, we err on the side of leniency w.r.t. this data structure's
	// contents
	name, ok, err := st.MaybeGet(workingSetMetaNameField)
	if err != nil {
		return nil, err
	}
	if !ok {
		name = types.String("not present")
	}

	email, ok, err := st.MaybeGet(workingSetMetaEmailField)
	if err != nil {
		return nil, err
	}
	if !ok {
		email = types.String("not present")
	}

	timestamp, ok, err := st.MaybeGet(workingSetMetaTimestampField)
	if err != nil {
		return nil, err
	}
	if !ok {
		timestamp = types.Uint(0)
	}

	description, ok, err := st.MaybeGet(workingSetMetaDescriptionField)
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
