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
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var ErrUnmergedWorkspaceDelete = errors.New("attempted to delete a workspace that is not fully merged into master; use `-f` to force")
var ErrCOWorkspaceDelete = errors.New("attempted to delete checked out workspace")

func CreateWorkspace(ctx context.Context, dEnv *env.DoltEnv, name, startPoint string) error {
	workRef := ref.NewWorkspaceRef(name)

	hasRef, err := dEnv.DoltDB.HasRef(ctx, workRef)
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

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())
	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewWorkspaceAtCommit(ctx, workRef, cm)
}

func IsWorkspace(ctx context.Context, dEnv *env.DoltEnv, str string) (bool, error) {
	dref := ref.NewWorkspaceRef(str)
	return dEnv.DoltDB.HasRef(ctx, dref)
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
		if ref.Equals(dEnv.RepoState.CWBHeadRef(), dref) {
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

func CheckoutWorkspace(ctx context.Context, dEnv *env.DoltEnv, wrkName string) error {
	dref := ref.NewWorkspaceRef(wrkName)

	hasRef, err := dEnv.DoltDB.HasRef(ctx, dref)
	if !hasRef {
		return doltdb.ErrWorkspaceNotFound
	}

	if ref.Equals(dEnv.RepoState.CWBHeadRef(), dref) {
		return doltdb.ErrAlreadyOnWorkspace
	}

	currRoots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), HeadRoot, WorkingRoot, StagedRoot)
	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec(wrkName)
	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	newRoot, err := cm.GetRootValue()
	if err != nil {
		return err
	}

	ssMap, err := newRoot.GetSuperSchemaMap(ctx)
	if err != nil {
		return err
	}

	fkMap, err := newRoot.GetForeignKeyCollectionMap(ctx)
	if err != nil {
		return err
	}

	conflicts := set.NewStrSet([]string{})
	wrkTblHashes, err := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[WorkingRoot], conflicts)
	if err != nil {
		return err
	}

	stgTblHashes, err := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[StagedRoot], conflicts)
	if err != nil {
		return err
	}
	if conflicts.Size() > 0 {
		return CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	wrkHash, err := writeRoot(ctx, dEnv, wrkTblHashes, ssMap, fkMap)
	if err != nil {
		return err
	}

	stgHash, err := writeRoot(ctx, dEnv, stgTblHashes, ssMap, fkMap)
	if err != nil {
		return err
	}

	unstagedDocs, err := GetUnstagedDocs(ctx, dEnv.DbData())
	if err != nil {
		return err
	}

	dEnv.RepoState.Head = ref.MarshalableRef{Ref: dref}
	dEnv.RepoState.Working = wrkHash.String()
	dEnv.RepoState.Staged = stgHash.String()

	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return err
	}

	return SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
}
