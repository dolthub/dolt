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

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type hooksDatabase struct {
	datas.Database
	postCommitHooks []CommitHook
}

// CommitHook is an abstraction for executing arbitrary commands after atomic database commits
type CommitHook interface {
	// Execute is arbitrary read-only function whose arguments are new Dataset commit into a specific Database
	Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error
	// HandleError is an bridge function to handle Execute errors
	HandleError(ctx context.Context, err error) error
	// SetLogger lets clients specify an output stream for HandleError
	SetLogger(ctx context.Context, wr io.Writer) error
}

func (db hooksDatabase) SetCommitHooks(ctx context.Context, postHooks []CommitHook) hooksDatabase {
	db.postCommitHooks = postHooks
	return db
}

func (db hooksDatabase) SetCommitHookLogger(ctx context.Context, wr io.Writer) hooksDatabase {
	for _, h := range db.postCommitHooks {
		h.SetLogger(ctx, wr)
	}
	return db
}

func (db hooksDatabase) PostCommitHooks() []CommitHook {
	return db.postCommitHooks
}

func (db hooksDatabase) ExecuteCommitHooks(ctx context.Context, ds datas.Dataset) {
	var err error
	for _, hook := range db.postCommitHooks {
		err = hook.Execute(ctx, ds, db)
		if err != nil {
			hook.HandleError(ctx, err)
		}
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
		db.ExecuteCommitHooks(ctx, commitDS)
	}
	return commitDS, workingSetDS, err
}
