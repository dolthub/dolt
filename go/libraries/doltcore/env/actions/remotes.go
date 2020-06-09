// Copyright 2019 Liquidata, Inc.
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
	"fmt"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/store/datas"
)

var ErrCantFF = errors.New("can't fast forward merge")

// Push will update a destination branch, in a given destination database if it can be done as a fast forward merge.
// This is accomplished first by verifying that the remote tracking reference for the source database can be updated to
// the given commit via a fast forward merge.  If this is the case, an attempt will be made to update the branch in the
// destination db to the given commit via fast forward move.  If that succeeds the tracking branch is updated in the
// source db.
func Push(ctx context.Context, dEnv *env.DoltEnv, mode ref.RefUpdateMode, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	var err error
	if mode == ref.FastForwardOnly {
		canFF, err := srcDB.CanFastForward(ctx, remoteRef, commit)

		if err != nil {
			return err
		} else if !canFF {
			return ErrCantFF
		}
	}

	err = destDB.PushChunks(ctx, dEnv.TempTableFilesDir(), srcDB, commit, progChan, pullerEventCh)

	if err != nil {
		return err
	}

	switch mode {
	case ref.ForceUpdate:
		err = destDB.SetHead(ctx, destRef, commit)
		if err != nil {
			return fmt.Errorf("error setting head: %s", err.Error())
		}
		err = srcDB.SetHead(ctx, remoteRef, commit)
		if err != nil {
			return fmt.Errorf("error setting head: %s", err.Error())
		}
	case ref.FastForwardOnly:
		err = destDB.FastForward(ctx, destRef, commit)
		if err != nil {
			return err
		}
		err = srcDB.FastForward(ctx, remoteRef, commit)
	}

	return err
}

// DeleteRemoteBranch validates targetRef is a branch on the remote database, and then deletes it, then deletes the
// remote tracking branch from the local database.
func DeleteRemoteBranch(ctx context.Context, targetRef ref.BranchRef, remoteRef ref.RemoteRef, localDB, remoteDB *doltdb.DoltDB) error {
	hasRef, err := remoteDB.HasRef(ctx, targetRef)

	if err != nil {
		return err
	}

	if hasRef {
		err = remoteDB.DeleteBranch(ctx, targetRef)
	}

	if err != nil {
		return err
	}

	err = localDB.DeleteBranch(ctx, remoteRef)

	if err != nil {
		return err
	}

	return nil
}

func Fetch(ctx context.Context, dEnv *env.DoltEnv, destRef ref.DoltRef, srcDB, destDB *doltdb.DoltDB, srcDBCommit *doltdb.Commit, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) error {
	return destDB.PullChunks(ctx, dEnv.TempTableFilesDir(), srcDB, srcDBCommit, progChan, pullerEventCh)
}

func Clone(ctx context.Context, srcDB, destDB *doltdb.DoltDB, eventCh chan<- datas.TableFileEvent) error {
	return srcDB.Clone(ctx, destDB, eventCh)
}
