package actions

import (
	"context"
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"

	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
)

var ErrCantFF = errors.New("can't fast forward merge")

// Push will update a destination branch, in a given destination database if it can be done as a fast forward merge.
// This is accomplished first by verifying that the remote tracking reference for the source database can be updated to
// the given commit via a fast forward merge.  If this is the case, an attempt will be made to update the branch in the
// destination db to the given commit via fast forward move.  If that succeeds the tracking branch is updated in the
// source db.
func Push(ctx context.Context, destRef ref.BranchRef, remoteRef ref.RemoteRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress) error {
	canFF, err := srcDB.CanFastForward(ctx, remoteRef, commit)

	if err != nil {
		return err
	} else if !canFF {
		return ErrCantFF
	}

	err = pantoerr.PanicToErrorNil("error pulling chunks", func() {
		destDB.PullChunks(ctx, srcDB, commit, progChan)
	})

	if err != nil {
		return err
	}

	err = destDB.FastForward(ctx, destRef, commit)

	if err != nil {
		return err
	}

	err = srcDB.FastForward(ctx, remoteRef, commit)

	return err
}

// DeleteRemoteBranch validates targetRef is a branch on the remote database, and then deletes it, then deletes the
// remote tracking branch from the local database.
func DeleteRemoteBranch(ctx context.Context, targetRef ref.BranchRef, remoteRef ref.RemoteRef, localDB, remoteDB *doltdb.DoltDB) error {
	var err error
	if remoteDB.HasRef(ctx, targetRef) {
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

func Fetch(ctx context.Context, destRef ref.DoltRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress) error {
	err := pantoerr.PanicToErrorNil("error pulling chunks", func() {
		destDB.PullChunks(ctx, srcDB, commit, progChan)
	})

	if err != nil {
		return err
	}

	return destDB.FastForward(ctx, destRef, commit)
}
