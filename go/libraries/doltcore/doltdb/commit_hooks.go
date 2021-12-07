// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/store/datas"
)

type PushOnWriteHook struct {
	destDB datas.Database
	tmpDir string
	out    io.Writer
}

var _ datas.CommitHook = (*PushOnWriteHook)(nil)

// NewPushOnWriteHook creates a ReplicateHook, parameterizaed by the backup database
// and a local tempfile for pushing
func NewPushOnWriteHook(destDB *DoltDB, tmpDir string) *PushOnWriteHook {
	return &PushOnWriteHook{destDB: destDB.db, tmpDir: tmpDir}
}

// Execute implements datas.CommitHook, replicates head updates to the destDb field
func (ph *PushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	return pushDataset(ctx, ph.destDB, db, ph.tmpDir, ds)
}

// HandleError implements datas.CommitHook
func (ph *PushOnWriteHook) HandleError(ctx context.Context, err error) error {
	if ph.out != nil {
		ph.out.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements datas.CommitHook
func (ph *PushOnWriteHook) SetLogger(ctx context.Context, wr io.Writer) error {
	ph.out = wr
	return nil
}

// replicate pushes a dataset from srcDB to destDB and force sets the destDB ref to the new dataset value
func pushDataset(ctx context.Context, destDB, srcDB datas.Database, tempTableDir string, ds datas.Dataset) error {
	stRef, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return err
	}
	if !ok {
		// No head ref, return
		return nil
	}

	rf, err := ref.Parse(ds.ID())
	if err != nil {
		return err
	}

	puller, err := datas.NewPuller(ctx, tempTableDir, defaultChunksPerTF, srcDB, destDB, stRef.TargetHash(), nil)
	if err == datas.ErrDBUpToDate {
		return nil
	} else if err != nil {
		return err
	}

	err = puller.Pull(ctx)
	if err != nil {
		return err
	}

	ds, err = destDB.GetDataset(ctx, rf.String())
	if err != nil {
		return err
	}

	_, err = destDB.SetHead(ctx, ds, stRef)
	return err
}

type PushArg struct {
	ds   datas.Dataset
	db   datas.Database
	hash hash.Hash
}

type AsyncPushOnWriteHook struct {
	out io.Writer
	ch  chan PushArg
}

const (
	asyncPushBufferSize = 20
	asyncPushProcessCommit = "async_push_process_commit"
	asyncPushSyncReplica = "async_push_sync_replica"

)

var _ datas.CommitHook = (*AsyncPushOnWriteHook)(nil)

// NewAsyncPushOnWriteHook creates a AsyncReplicateHook
func NewAsyncPushOnWriteHook(ctx context.Context, bThreads *sql.BackgroundThreads, destDB *DoltDB, tmpDir string, logger io.Writer) *AsyncPushOnWriteHook {
	ch := make(chan PushArg, asyncPushBufferSize)
	RunAsyncReplicationThreads(ctx, bThreads, ch, destDB, tmpDir, logger)
	return &AsyncPushOnWriteHook{ch: ch}
}

// Execute implements datas.CommitHook, replicates head updates to the destDb field
func (ah *AsyncPushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	rf, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return ErrHashNotFound
	}
	if !ok {
		return ErrHashNotFound
	}

	select {
	case ah.ch <- PushArg{ds: ds, db: db, hash: rf.TargetHash()}:
	case <-ctx.Done():
		ah.ch <- PushArg{ds: ds, db: db, hash: rf.TargetHash()}
		return ctx.Err()
	}
	return nil
}

// HandleError implements datas.CommitHook
func (ah *AsyncPushOnWriteHook) HandleError(ctx context.Context, err error) error {
	if ah.out != nil {
		ah.out.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements datas.CommitHook
func (ah *AsyncPushOnWriteHook) SetLogger(ctx context.Context, wr io.Writer) error {
	ah.out = wr
	return nil
}

type LogHook struct {
	msg []byte
	out io.Writer
}

var _ datas.CommitHook = (*LogHook)(nil)

// NewLogHook is a noop that logs to a writer when invoked
func NewLogHook(msg []byte) *LogHook {
	return &LogHook{msg: msg}
}

// Execute implements datas.CommitHook, writes message to log channel
func (lh *LogHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	if lh.out != nil {
		_, err := lh.out.Write(lh.msg)
		return err
	}
	return nil
}

// HandleError implements datas.CommitHook
func (lh *LogHook) HandleError(ctx context.Context, err error) error {
	if lh.out != nil {
		lh.out.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements datas.CommitHook
func (lh *LogHook) SetLogger(ctx context.Context, wr io.Writer) error {
	lh.out = wr
	return nil
}

func RunAsyncReplicationThreads(ctx context.Context, bThreads *sql.BackgroundThreads, ch chan PushArg, destDB *DoltDB, tmpDir string, logger io.Writer) {
	mu := &sync.Mutex{}
	var newHeads = make(map[string]PushArg, asyncPushBufferSize)

	// stop first go routine before second
	newCtx, stop := context.WithCancel(context.Background())
	bThreads.Add(asyncPushProcessCommit, func(ctx context.Context) {
		for {
			select {
			case p, ok := <-ch:
				if !ok {
					return
				}
				mu.Lock()
				newHeads[p.ds.ID()] = p
				mu.Unlock()
			case <-ctx.Done():
				stop()
				return
			}
		}
	})

	bThreads.Add(asyncPushSyncReplica, func(ctx context.Context) {
		defer close(ch)
		var latestHeads = make(map[string]hash.Hash, asyncPushBufferSize)
		var newHeadsCopy = make(map[string]PushArg, asyncPushBufferSize)
		flush := func() {
			mu.Lock()
			if len(newHeads) == 0 {
				mu.Unlock()
				return
			}
			for k, v := range newHeads {
				newHeadsCopy[k] = v
			}
			mu.Unlock()
			for id, newCm := range newHeadsCopy {
				if latest, ok := latestHeads[id]; !ok || latest != newCm.hash {
					// use background context to drain after sql context is canceled
					err := pushDataset(context.Background(), destDB.db, newCm.db, tmpDir, newCm.ds)
					if err != nil {
						logger.Write([]byte("replication failed: " + err.Error()))
					}
					latestHeads[id] = newCm.hash
				}
			}
		}

		for {
			select {
			case <-newCtx.Done():
				flush()
				return
			default:
				flush()
			}
		}
	})
}
