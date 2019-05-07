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

func Push(ctx context.Context, branchRef, remoteRef ref.DoltRef, srcDB, destDB *doltdb.DoltDB, commit *doltdb.Commit, progChan chan datas.PullProgress) error {
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

	err = destDB.FastForward(ctx, branchRef, commit)

	if err != nil {
		return err
	}

	err = srcDB.FastForward(ctx, remoteRef, commit)

	return err
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
