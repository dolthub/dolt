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

package doltdb

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// HeadUpdate builds one dataset update to publish in an atomic batch.
type HeadUpdate interface {
	BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error)
}

// WorkingSetUpdate replaces a working set, provided its current hash equals PrevHash.
type WorkingSetUpdate struct {
	WorkingSet *WorkingSet
	PrevHash   hash.Hash
	Meta       *datas.WorkingSetMeta
}

var _ HeadUpdate = WorkingSetUpdate{}

func (u WorkingSetUpdate) BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error) {
	spec, err := u.WorkingSet.writeValues(ctx, ddb, u.Meta, false)
	if err != nil {
		return nil, err
	}
	return datas.WorkingSetUpdate{
		WorkingSetDS: u.WorkingSet.Ref().String(),
		WorkingSet:   *spec,
		PrevWsHash:   u.PrevHash,
	}, nil
}

// BranchHeadUpdate creates a commit and updates its branch head.
type BranchHeadUpdate struct {
	HeadRef       ref.DoltRef
	Root          RootValue
	CommitOptions datas.CommitOptions
	// ExpectedHead is the head used to prepare the commit. When supplied, both
	// this hash and the dataset snapshot taken by BuildDatasetUpdate are checked.
	ExpectedHead *hash.Hash
	// WorkingSetRef optionally locks an associated working set at PrevWsHash.
	// A zero reference omits this additional lock; the branch head is always checked.
	WorkingSetRef ref.WorkingSetRef
	PrevWsHash    hash.Hash
}

var _ HeadUpdate = BranchHeadUpdate{}

func (u BranchHeadUpdate) BuildDatasetUpdate(ctx context.Context, ddb *DoltDB) (datas.DatasetUpdate, error) {
	ds, err := ddb.db.GetDataset(ctx, u.HeadRef.String())
	if err != nil {
		return nil, err
	}
	var wsID string
	if u.WorkingSetRef.GetPath() != "" {
		wsID = u.WorkingSetRef.String()
	}
	return datas.CommitUpdate{
		CommitDS:     ds,
		ExpectedHead: u.ExpectedHead,
		CommitOpts:   u.CommitOptions,
		WorkingSetDS: wsID,
		PrevWsHash:   u.PrevWsHash,
		RootVal:      u.Root.NomsValue(),
	}, nil
}

// CommitHeadUpdates publishes all updates in one storage transaction. Each returned
// hash is the new dataset head for the input at the same index.
func (ddb *DoltDB) CommitHeadUpdates(
	ctx context.Context,
	updates []HeadUpdate,
	replicationStatus *ReplicationStatusController,
) ([]hash.Hash, error) {
	pending := make([]datas.DatasetUpdate, len(updates))
	for i, update := range updates {
		var err error
		pending[i], err = update.BuildDatasetUpdate(ctx, ddb)
		if err != nil {
			return nil, err
		}
	}

	datasets, err := ddb.db.withReplicationStatusController(replicationStatus).CommitDatasets(ctx, pending)
	if err != nil {
		return nil, err
	}
	heads := make([]hash.Hash, len(datasets))
	for i, ds := range datasets {
		heads[i], _ = ds.MaybeHeadAddr()
		if ds.IsWorkingSet() {
			// The lock hash identifies the exact previous value, even if another writer
			// changes this working set again before listeners run.
			if err := ddb.notifyWorkingRootUpdated(ctx, ds, pending[i].LockPrevHash()); err != nil {
				logrus.Errorf("error notifying working root listeners of update: %s", err)
			}
		}
	}
	return heads, nil
}

// notifyWorkingRootUpdated infers notifications from a published working-set dataset.
// Listener failures are non-fatal: the batch has already been committed.
func (ddb *DoltDB) notifyWorkingRootUpdated(ctx context.Context, ds datas.Dataset, previous hash.Hash) error {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok || len(DatabaseUpdateListeners) == 0 {
		return nil
	}
	headRef, err := ref.NewWorkingSetRef(ds.ID()).ToHeadRef()
	if err != nil {
		return err
	}
	if headRef.GetType() != ref.BranchRefType {
		return nil
	}
	var prevRoot RootValue
	if previous.IsEmpty() {
		// New branches have no prior working root; listeners receive an empty root.
		prevRoot, err = EmptyRootValue(ctx, ddb.vrw, ddb.ns)
	} else {
		value, readErr := datas.LoadRootNomsValueFromRootIshAddr(ctx, ddb.vrw, previous)
		if readErr != nil {
			return readErr
		}
		prevRoot, err = NewRootValue(ctx, ddb.vrw, ddb.ns, value)
	}
	if err != nil {
		return err
	}
	ws, err := ds.HeadWorkingSet()
	if err != nil {
		return err
	}
	root, err := ddb.ReadRootValue(ctx, ws.WorkingAddr)
	if err != nil {
		return err
	}
	for _, listener := range DatabaseUpdateListeners {
		if err := listener.WorkingRootUpdated(sqlCtx, ddb.databaseName, headRef.GetPath(), prevRoot, root); err != nil {
			logrus.Errorf("error notifying working root listener of update: %s", err)
		}
	}
	return nil
}
