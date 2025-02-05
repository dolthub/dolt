// Copyright 2022 Dolthub, Inc.
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
	"io"
	"sync"

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type hooksDatabase struct {
	datas.Database
	db    *DoltDB
	hooks []CommitHook
	rsc   *ReplicationStatusController
}

// CommitHook is an abstraction for executing arbitrary commands after atomic database commits
type CommitHook interface {
	// Execute is arbitrary read-only function whose arguments are new Dataset commit into a specific Database
	Execute(ctx context.Context, ds datas.Dataset, db *DoltDB) (func(context.Context) error, error)
	// HandleError is an bridge function to handle Execute errors
	HandleError(ctx context.Context, err error) error
	// SetLogger lets clients specify an output stream for HandleError
	SetLogger(ctx context.Context, wr io.Writer) error
	// ExecuteForWorkingSets returns whether or not the hook should be executed for working set updates
	ExecuteForWorkingSets() bool
}

// NotifyWaitFailedCommitHook is an optional interface that can be implemented by CommitHooks.
// If a commit hook supports this interface, it can be notified if waiting for
// replication in the callback returned by |Execute| failed to complete in time
// or returned an error.
type NotifyWaitFailedCommitHook interface {
	NotifyWaitFailed()
}

func (db hooksDatabase) SetCommitHooks(ctx context.Context, postHooks []CommitHook) hooksDatabase {
	db.hooks = make([]CommitHook, len(postHooks))
	copy(db.hooks, postHooks)
	return db
}

func (db hooksDatabase) SetCommitHookLogger(ctx context.Context, wr io.Writer) hooksDatabase {
	for _, h := range db.hooks {
		h.SetLogger(ctx, wr)
	}
	return db
}

func (db hooksDatabase) withReplicationStatusController(rsc *ReplicationStatusController) hooksDatabase {
	db.rsc = rsc
	return db
}

func (db hooksDatabase) PostCommitHooks() []CommitHook {
	toret := make([]CommitHook, len(db.hooks))
	copy(toret, db.hooks)
	return toret
}

func (db hooksDatabase) ExecuteCommitHooks(ctx context.Context, ds datas.Dataset, onlyWS bool) {
	var wg sync.WaitGroup
	rsc := db.rsc
	var ioff int
	if rsc != nil {
		ioff = len(rsc.Wait)
		rsc.Wait = append(rsc.Wait, make([]func(context.Context) error, len(db.hooks))...)
		rsc.NotifyWaitFailed = append(rsc.NotifyWaitFailed, make([]func(), len(db.hooks))...)
	}
	for il, hook := range db.hooks {
		if !onlyWS || hook.ExecuteForWorkingSets() {
			i := il
			hook := hook
			wg.Add(1)
			go func() {
				defer wg.Done()
				f, err := hook.Execute(ctx, ds, db.db)
				if err != nil {
					hook.HandleError(ctx, err)
				}
				if rsc != nil {
					rsc.Wait[i+ioff] = f
					if nf, ok := hook.(NotifyWaitFailedCommitHook); ok {
						rsc.NotifyWaitFailed[i+ioff] = nf.NotifyWaitFailed
					} else {
						rsc.NotifyWaitFailed[i+ioff] = func() {}
					}
				}
			}()
		}
	}
	wg.Wait()
	if rsc != nil {
		j := ioff
		for i := ioff; i < len(rsc.Wait); i++ {
			if rsc.Wait[i] != nil {
				rsc.Wait[j] = rsc.Wait[i]
				rsc.NotifyWaitFailed[j] = rsc.NotifyWaitFailed[i]
				j++
			}
		}
		rsc.Wait = rsc.Wait[:j]
		rsc.NotifyWaitFailed = rsc.NotifyWaitFailed[:j]
	}
}

func (db hooksDatabase) CommitWithWorkingSet(
	ctx context.Context,
	commitDS, workingSetDS datas.Dataset,
	val types.Value, workingSetSpec datas.WorkingSetSpec,
	prevWsHash hash.Hash, opts datas.CommitOptions,
) (datas.Dataset, datas.Dataset, error) {
	commitDS, workingSetDS, err := db.Database.CommitWithWorkingSet(
		ctx,
		commitDS,
		workingSetDS,
		val,
		workingSetSpec,
		prevWsHash,
		opts)
	if err == nil {
		db.ExecuteCommitHooks(ctx, commitDS, false)
	}
	return commitDS, workingSetDS, err
}

func (db hooksDatabase) Commit(ctx context.Context, ds datas.Dataset, v types.Value, opts datas.CommitOptions) (datas.Dataset, error) {
	ds, err := db.Database.Commit(ctx, ds, v, opts)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}

func (db hooksDatabase) WriteCommit(ctx context.Context, ds datas.Dataset, commit *datas.Commit) (datas.Dataset, error) {
	ds, err := db.Database.WriteCommit(ctx, ds, commit)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}

func (db hooksDatabase) SetHead(ctx context.Context, ds datas.Dataset, newHeadAddr hash.Hash, ws string) (datas.Dataset, error) {
	ds, err := db.Database.SetHead(ctx, ds, newHeadAddr, ws)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}

func (db hooksDatabase) FastForward(ctx context.Context, ds datas.Dataset, newHeadAddr hash.Hash, workingSetPath string) (datas.Dataset, error) {
	ds, err := db.Database.FastForward(ctx, ds, newHeadAddr, workingSetPath)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}

func (db hooksDatabase) Delete(ctx context.Context, ds datas.Dataset, workingSetPath string) (datas.Dataset, error) {
	ds, err := db.Database.Delete(ctx, ds, workingSetPath)
	if err == nil {
		db.ExecuteCommitHooks(ctx, datas.NewHeadlessDataset(ds.Database(), ds.ID()), false)
	}
	return ds, err
}

func (db hooksDatabase) UpdateWorkingSet(ctx context.Context, ds datas.Dataset, workingSet datas.WorkingSetSpec, prevHash hash.Hash) (datas.Dataset, error) {
	ds, err := db.Database.UpdateWorkingSet(ctx, ds, workingSet, prevHash)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, true)
	}
	return ds, err
}

func (db hooksDatabase) Tag(ctx context.Context, ds datas.Dataset, commitAddr hash.Hash, opts datas.TagOptions) (datas.Dataset, error) {
	ds, err := db.Database.Tag(ctx, ds, commitAddr, opts)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}

func (db hooksDatabase) SetTuple(ctx context.Context, ds datas.Dataset, val []byte) (datas.Dataset, error) {
	ds, err := db.Database.SetTuple(ctx, ds, val)
	if err == nil {
		db.ExecuteCommitHooks(ctx, ds, false)
	}
	return ds, err
}
