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

package actions

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

var ErrUnmergedWorkspaceDelete = errors.New("attempted to delete a workspace that is not fully merged into its parent; use `-f` to force")
var ErrCOWorkspaceDelete = errors.New("attempted to delete checked out workspace")
var ErrBranchNameExists = errors.New("workspace name must not be existing branch name")

func CreateWorkspace(ctx context.Context, dEnv *env.DoltEnv, name, startPoint string) error {
	headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return nil
	}
	return CreateWorkspaceOnDB(ctx, dEnv.DoltDB(ctx), name, startPoint, headRef)
}

func CreateWorkspaceOnDB(ctx context.Context, ddb *doltdb.DoltDB, name, startPoint string, headRef ref.DoltRef) error {
	isBranch, err := IsBranch(ctx, ddb, name)
	if err != nil {
		return err
	}
	if isBranch {
		return ErrBranchNameExists
	}

	if !doltdb.IsValidUserBranchName(name) {
		return doltdb.ErrInvWorkspaceName
	}

	workRef := ref.NewWorkspaceRef(name)

	hasRef, err := ddb.HasRef(ctx, workRef)
	if err != nil {
		return err
	}
	if hasRef {
		return ErrAlreadyExists
	}

	cs, err := doltdb.NewCommitSpec(startPoint)
	if err != nil {
		return err
	}

	optCmt, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return doltdb.ErrGhostCommitEncountered
	}

	return ddb.NewWorkspaceAtCommit(ctx, workRef, cm)
}

func IsWorkspaceOnDB(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	dref := ref.NewWorkspaceRef(str)
	return ddb.HasRef(ctx, dref)
}

func IsWorkspace(ctx context.Context, dEnv *env.DoltEnv, str string) (bool, error) {
	return IsWorkspaceOnDB(ctx, dEnv.DoltDB(ctx), str)
}

func DeleteWorkspace(ctx context.Context, dEnv *env.DoltEnv, workspaceName string, opts DeleteOptions) error {
	var dref ref.DoltRef
	if opts.Remote {
		var err error
		dref, err = ref.NewRemoteRefFromPathStr(workspaceName)
		if err != nil {
			return err
		}
	} else {
		dref = ref.NewWorkspaceRef(workspaceName)
		headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
		if err != nil {
			return err
		}
		if ref.Equals(headRef, dref) {
			return ErrCOWorkspaceDelete
		}
	}

	return DeleteWorkspaceOnDB(ctx, dEnv, dref, opts)
}

func DeleteWorkspaceOnDB(ctx context.Context, dEnv *env.DoltEnv, dref ref.DoltRef, opts DeleteOptions) error {
	ddb := dEnv.DoltDB(ctx)
	hasRef, err := ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrWorkspaceNotFound
	}

	if !opts.Force && !opts.Remote {
		ms, err := doltdb.NewCommitSpec(env.GetDefaultInitBranch(dEnv.Config))
		if err != nil {
			return err
		}

		optCmt, err := ddb.Resolve(ctx, ms, nil)
		if err != nil {
			return err
		}
		m, ok := optCmt.ToCommit()
		if !ok {
			return doltdb.ErrGhostCommitEncountered
		}

		cs, err := doltdb.NewCommitSpec(dref.String())
		if err != nil {
			return err
		}

		optCmt, err = ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}
		cm, ok := optCmt.ToCommit()
		if !ok {
			return doltdb.ErrGhostCommitEncountered
		}

		isMerged, _ := m.CanFastReverseTo(ctx, cm)
		if err != nil && err != doltdb.ErrUpToDate {
			return err
		}
		if !isMerged {
			return ErrUnmergedWorkspaceDelete
		}
	}

	return ddb.DeleteWorkspace(ctx, dref)
}
