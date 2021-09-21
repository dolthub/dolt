package doltdb

import (
	"context"
	"io"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/store/datas"
)

type ReplicateHook struct {
	//destDB datas.Database
	destDB *DoltDB
	//r env.Remote
	tmpDir string
}

func NewReplicateHook(destDB *DoltDB, tmpDir string) *ReplicateHook {
	return &ReplicateHook{destDB: destDB, tmpDir: tmpDir}
}

// Execute implements datas.CommitHook, replicates head updates to the destDb field
func (rh *ReplicateHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	return replicate(ctx, rh.destDB, db, rh.tmpDir, ds)
}

// HandleError implements datas.CommitHook
func (rh *ReplicateHook) HandleError(ctx context.Context, err error, wr io.Writer) error {
	return nil
}

func replicate(ctx context.Context, destDB *DoltDB, srcDB datas.Database, tempTableDir string, ds datas.Dataset) error {
	stRef, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return err
	}
	if !ok {
		// No head ref, return
		return nil
	}

	rf, err := ref.Parse(ds.ID())
	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, pullerEventCh := runProgFuncs(newCtx)
	puller, err := datas.NewPuller(ctx, tempTableDir, defaultChunksPerTF, srcDB, destDB.db, stRef.TargetHash(), pullerEventCh)

	if err == datas.ErrDBUpToDate {
		return nil
	} else if err != nil {
		return err
	}

	err = puller.Pull(ctx)
	if err != nil {
		return err
	}

	stopProgFuncs(cancelFunc, wg, progChan, pullerEventCh)
	if err != nil {
		return err
	}

	err = destDB.SetHead(ctx, rf, stRef)
	if err != nil {
		return err
	}

	return nil
}

func pullerProgFunc(ctx context.Context, pullerEventCh <-chan datas.PullerEvent) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-pullerEventCh:
		default:
		}
	}
}

func progFunc(ctx context.Context, progChan <-chan datas.PullProgress) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-progChan:
		default:
		}
	}
}

func runProgFuncs(ctx context.Context) (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent) {
	pullerEventCh := make(chan datas.PullerEvent)
	progChan := make(chan datas.PullProgress)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(ctx, progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(ctx, pullerEventCh)
	}()

	return wg, progChan, pullerEventCh
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) {
	cancel()
	close(progChan)
	close(pullerEventCh)
	wg.Wait()
}
