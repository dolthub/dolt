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
	workingSetName = "WorkingSet"
)

type WorkingSetMeta struct {
	Name        string
	Email       string
	Description string
	Timestamp   uint64
}

type WorkingSetSpec struct {
	Meta        *WorkingSetMeta
	MergeState  *MergeState
	RebaseState *RebaseState
	WorkingRoot types.Ref
	StagedRoot  types.Ref
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
func newWorkingSet(ctx context.Context, db *database, workingSetSpec WorkingSetSpec) (hash.Hash, error) {
	meta := workingSetSpec.Meta
	workingRef := workingSetSpec.WorkingRoot
	stagedRef := workingSetSpec.StagedRoot
	mergeState := workingSetSpec.MergeState
	rebaseState := workingSetSpec.RebaseState

	stagedAddr := stagedRef.TargetHash()
	data := workingset_flatbuffer(workingRef.TargetHash(), &stagedAddr, mergeState, rebaseState, meta)

	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}

	ref, err := types.ToRefOfValue(r, db.Format())
	if err != nil {
		return hash.Hash{}, err
	}

	return ref.TargetHash(), nil
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
		serial.RebaseStateAddLastAttemptedStep(builder, rebaseState.lastAttemptedStep)
		serial.RebaseStateAddRebasingStarted(builder, rebaseState.rebasingStarted)
		serial.RebaseStateAddSkipVerification(builder, rebaseState.skipVerification)
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
	preMergeWorking types.Ref,
	commit *Commit,
	commitSpecStr string,
	unmergableTables []string,
	isCherryPick bool,
) (*MergeState, error) {
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
}

func NewRebaseState(preRebaseWorkingRoot hash.Hash, commitAddr hash.Hash, branch string, commitBecomesEmptyHandling uint8, emptyCommitHandling uint8, lastAttemptedStep float32, rebasingStarted bool, skipVerification bool) *RebaseState {
	return &RebaseState{
		preRebaseWorkingAddr:       &preRebaseWorkingRoot,
		ontoCommitAddr:             &commitAddr,
		branch:                     branch,
		commitBecomesEmptyHandling: commitBecomesEmptyHandling,
		emptyCommitHandling:        emptyCommitHandling,
		lastAttemptedStep:          lastAttemptedStep,
		rebasingStarted:            rebasingStarted,
		skipVerification:           skipVerification,
	}
}
