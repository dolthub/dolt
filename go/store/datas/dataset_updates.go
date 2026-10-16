// Copyright 2026 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// valueBuilder is a limited subset of Database used to construct values to commit
type valueBuilder interface {
	BuildNewCommit(ctx context.Context, ds Dataset, v types.Value, opts CommitOptions) (*Commit, error)
	WriteValue(ctx context.Context, v types.Value) (types.Ref, error)
	Format() *types.NomsBinFormat
}

// DatasetUpdate is an update to a single dataset to commit as part of an atomic batch.
// Implementors must be defined in this package.
type DatasetUpdate interface {
	// DatasetID returns the ID of the dataset to update.
	DatasetID() string
	// build writes an immutable value and returns the metadata object that will be used to commit it
	build(ctx context.Context, vb valueBuilder) (writtenDatasetUpdate, error)
}

// writtenDatasetUpdate contains a written value and its publication preconditions.
// Preconditions are checked against the original dataset map on every root update attempt.
type writtenDatasetUpdate struct {
	NewHead       hash.Hash
	Preconditions []datasetCommitPrecondition
}

// datasetCommitPrecondition compares a dataset's current head with ExpectedHead.
// A zero ExpectedHead denotes an absent dataset. NotEqual reverses the comparison.
// OnFailure defaults to ErrOptimisticLockFailed when nil.
type datasetCommitPrecondition struct {
	DatasetID    string
	ExpectedHead hash.Hash
	NotEqual     bool
	OnFailure    error
}

// WorkingSetUpdate is a DatasetUpdate that updates a working set.
type WorkingSetUpdate struct {
	// WorkingSetDS is the working set Dataset to update with a new working set.
	WorkingSetDS string
	// WorkingSet is the new working set to write to the WorkingSetDS.
	WorkingSet WorkingSetSpec
	// PrevWsHash is the expected hash of the current working set for the WorkingSetDS.
	// If the current working set does not match this hash, the update will fail with ErrOptimisticLockFailed.
	PrevWsHash hash.Hash
}

var _ DatasetUpdate = &WorkingSetUpdate{}

func (w WorkingSetUpdate) DatasetID() string {
	return w.WorkingSetDS
}

func (w WorkingSetUpdate) build(ctx context.Context, db valueBuilder) (writtenDatasetUpdate, error) {
	head, err := newWorkingSet(ctx, db, w.WorkingSet)
	if err != nil {
		return writtenDatasetUpdate{}, err
	}

	return writtenDatasetUpdate{
		NewHead: head,
		Preconditions: []datasetCommitPrecondition{{
			DatasetID:    w.WorkingSetDS,
			ExpectedHead: w.PrevWsHash,
			OnFailure:    ErrOptimisticLockFailed,
		}},
	}, nil
}

// CommitUpdate updates a branch head. It uses two locks: the branch head value that was used to prepare the
// commit provided, as well as the working set value that was included in that commit. Both must be current for the
// commit to succeed.
type CommitUpdate struct {
	// CommitDS is the Dataset to update with a new commit.
	CommitDS Dataset
	// ExpectedHead, when nonzero, must match the dataset snapshot used to build the commit.
	// Without this check, it would be possible for another writer to change the branch head before we acquire the update
	// lock, which invalidates the invariant that we prepared a commit value using the current HEAD as parent.
	ExpectedHead hash.Hash
	// CommitOpts are the options used to build the commit.
	CommitOpts CommitOptions
	// WorkingSetDS, when non-empty, is the Dataset containing the working set that was used to build the commit.
	WorkingSetDS string
	// PrevWsHash, when nonzero, must match the current working set at write time for commit to succeed.
	PrevWsHash hash.Hash
	// RootVal is the value to commit.
	RootVal types.Value
}

func (c CommitUpdate) DatasetID() string {
	return c.CommitDS.ID()
}

func (c CommitUpdate) build(ctx context.Context, db valueBuilder) (writtenDatasetUpdate, error) {
	// Prepend the current head hash to the list of parents if one was provided. This is only necessary if parents were
	// provided because we fill it in automatically in buildNewCommit otherwise.
	if len(c.CommitOpts.Parents) > 0 && c.CommitOpts.AmendedCommit.IsEmpty() && !c.CommitOpts.Force {
		headHash, ok := c.CommitDS.MaybeHeadAddr()
		if ok {
			if !hasParentHash(c.CommitOpts, headHash) {
				c.CommitOpts.Parents = append([]hash.Hash{headHash}, c.CommitOpts.Parents...)
			}
		}
	}

	commit, err := db.BuildNewCommit(ctx, c.CommitDS, c.RootVal, c.CommitOpts)
	if err != nil {
		return writtenDatasetUpdate{}, err
	}

	commitRef, err := db.WriteValue(ctx, commit.NomsValue())
	if err != nil {
		return writtenDatasetUpdate{}, err
	}

	var preconditions []datasetCommitPrecondition
	if c.WorkingSetDS != "" {
		preconditions = append(preconditions, datasetCommitPrecondition{
			DatasetID:    c.WorkingSetDS,
			ExpectedHead: c.PrevWsHash,
			OnFailure:    ErrOptimisticLockFailed,
		})
	}

	if !c.ExpectedHead.IsEmpty() {
		preconditions = append(preconditions, datasetCommitPrecondition{
			DatasetID:    c.DatasetID(),
			ExpectedHead: c.ExpectedHead,
			OnFailure:    ErrMergeNeeded,
		})
	}

	return writtenDatasetUpdate{
		NewHead:       commitRef.TargetHash(),
		Preconditions: preconditions,
	}, nil
}

var _ DatasetUpdate = &CommitUpdate{}

// PrebuiltCommitUpdate publishes an already constructed commit without changing its parents.
type PrebuiltCommitUpdate struct {
	CommitDS Dataset
	Commit   *Commit
}

var _ DatasetUpdate = PrebuiltCommitUpdate{}

func (c PrebuiltCommitUpdate) DatasetID() string { return c.CommitDS.ID() }

func (c PrebuiltCommitUpdate) build(ctx context.Context, db valueBuilder) (writtenDatasetUpdate, error) {
	r, err := db.WriteValue(ctx, c.Commit.NomsValue())
	if err != nil {
		return writtenDatasetUpdate{}, err
	}

	expected, _ := c.CommitDS.MaybeHeadAddr()
	return writtenDatasetUpdate{
		NewHead: r.TargetHash(),
		Preconditions: []datasetCommitPrecondition{
			{
				DatasetID:    c.DatasetID(),
				ExpectedHead: expected,
				OnFailure:    ErrMergeNeeded,
			},
			{
				DatasetID:    c.DatasetID(),
				ExpectedHead: r.TargetHash(),
				NotEqual:     true,
				OnFailure:    ErrAlreadyCommitted,
			},
		},
	}, nil
}
