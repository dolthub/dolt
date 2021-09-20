package doltdb

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/dolthub/dolt/go/store/datas"
)

//const BackupRemoteKey = "DOLT_BACKUP_REMOTE"
//var postCommitHooks []datas.UpdateHook
//
//func init() {
//	backupUrl := os.Getenv(BackupRemoteKey)
//	if backupUrl != "" {
//		// parse remote
//		ctx := context.Background()
//		r, srcDb, err := env.CreateRemote(ctx, "backup", backupUrl, nil, nil)
//		if err != nil {
//			return
//		}
//		// build destDB
//
//		postCommitHooks = append(postCommitHooks, func(ctx context.Context, ds datas.Dataset) error {
//			headRef, _, _ := ds.MaybeHeadRef()
//			//id := ds.ID()
//			//return backup.Backup(ctx, srcDb, "temp", r, headRef, nil, nil)
//		})
//	}
//}

type ReplicateHook struct {
	destDB datas.Database
	tmpDir string
}

func NewReplicateHook(destDB *DoltDB) *ReplicateHook {
	return &ReplicateHook{
		destDB: destDB.db,
		tmpDir: "",
	}
}

// Execute implements datas.CommitHook, replicates head updates to the destDb field
func (rh *ReplicateHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	return replicate(ctx, db, rh.destDB, rh.tmpDir, ds)
}

// HandleError implements datas.CommitHook
func (rh *ReplicateHook) HandleError(ctx context.Context, err error, wr io.Writer) error {
	return nil
}

// WithTempfile implements datas.CommitHook
func (rh *ReplicateHook) WithTempfile(t string) datas.CommitHook {
	rh.tmpDir = t
	return rh
}

func replicate(ctx context.Context, srcDB, destDB datas.Database, tempTableDir string, ds datas.Dataset) error {
	stRef, ok, err := ds.MaybeHeadRef()
	t, _ := stRef.TargetType()
	fmt.Print(t)
	if err != nil {
		return err
	}
	if !ok {
		panic("max fix")
	}

	//rf, err := ref.Parse(ds.ID())
	//if err != nil {
	//	return err
	//}
	newCtx, cancelFunc := context.WithCancel(ctx)
	wg, progChan, pullerEventCh := runProgFuncs(newCtx)
	puller, err := datas.NewPuller(ctx, tempTableDir, defaultChunksPerTF, srcDB, destDB, stRef.TargetHash(), pullerEventCh)

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

	ds, err = destDB.SetHead(ctx, ds, stRef)
	if err != nil {
		return err
	}

	return nil
}

func pullerProgFunc(ctx context.Context, pullerEventCh <-chan datas.PullerEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
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
		select {
		case <-ctx.Done():
			return
		default:
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
