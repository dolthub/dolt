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

var ErrUnmergedWorkspaceDelete = errors.New("attempted to delete a workspace that is not fully merged into master; use `-f` to force")
var ErrCOWorkspaceDelete = errors.New("attempted to delete checked out workspace")
var ErrBranchNameExists = errors.New("workspace name must not be existing branch name")

func CreateWorkspace(ctx context.Context, dEnv *env.DoltEnv, name, startPoint string) error {
	return CreateWorkspaceOnDB(ctx, dEnv.DoltDB, name, startPoint, dEnv.RepoStateReader().CWBHeadRef())
}

func CreateWorkspaceOnDB(ctx context.Context, ddb *doltdb.DoltDB, name, startPoint string, headRef ref.DoltRef) error {
	isBranch, err := IsBranchOnDB(ctx, ddb, name)
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

	cm, err := ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return err
	}

	return ddb.NewWorkspaceAtCommit(ctx, workRef, cm)
}

func IsWorkspaceOnDB(ctx context.Context, ddb *doltdb.DoltDB, str string) (bool, error) {
	dref := ref.NewWorkspaceRef(str)
	return ddb.HasRef(ctx, dref)
}

func IsWorkspace(ctx context.Context, dEnv *env.DoltEnv, str string) (bool, error) {
	return IsWorkspaceOnDB(ctx, dEnv.DoltDB, str)
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
		if ref.Equals(dEnv.RepoStateReader().CWBHeadRef(), dref) {
			return ErrCOWorkspaceDelete
		}
	}

	return DeleteWorkspaceOnDB(ctx, dEnv.DoltDB, dref, opts)
}

func DeleteWorkspaceOnDB(ctx context.Context, ddb *doltdb.DoltDB, dref ref.DoltRef, opts DeleteOptions) error {
	hasRef, err := ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return doltdb.ErrWorkspaceNotFound
	}

	if !opts.Force && !opts.Remote {
		ms, err := doltdb.NewCommitSpec("master")
		if err != nil {
			return err
		}

		master, err := ddb.Resolve(ctx, ms, nil)
		if err != nil {
			return err
		}

		cs, err := doltdb.NewCommitSpec(dref.String())
		if err != nil {
			return err
		}

		cm, err := ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}

		isMerged, _ := master.CanFastReverseTo(ctx, cm)
		if err != nil && err != doltdb.ErrUpToDate {
			return err
		}
		if !isMerged {
			return ErrUnmergedWorkspaceDelete
		}
	}

	return ddb.DeleteWorkspace(ctx, dref)
}
