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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type PushOnWriteHook struct {
	destDB datas.Database
	tmpDir string
	out    io.Writer
	fmt    *types.NomsBinFormat
}

var _ CommitHook = (*PushOnWriteHook)(nil)

// NewPushOnWriteHook creates a ReplicateHook, parameterizaed by the backup database
// and a local tempfile for pushing
func NewPushOnWriteHook(destDB *DoltDB, tmpDir string) *PushOnWriteHook {
	return &PushOnWriteHook{
		destDB: destDB.db,
		tmpDir: tmpDir,
		fmt:    destDB.Format(),
	}
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ph *PushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	return pushDataset(ctx, ph.destDB, db, ds, ph.tmpDir)
}

func pushDataset(ctx context.Context, destDB, srcDB datas.Database, ds datas.Dataset, tmpDir string) error {
	addr, ok := ds.MaybeHeadAddr()
	if !ok {
		_, err := destDB.Delete(ctx, ds)
		return err
	}

	err := pullHash(ctx, destDB, srcDB, []hash.Hash{addr}, tmpDir, nil, nil)
	if err != nil {
		return err
	}

	rf, err := ref.Parse(ds.ID())
	if err != nil {
		return err
	}

	ds, err = destDB.GetDataset(ctx, rf.String())
	if err != nil {
		return err
	}

	_, err = destDB.SetHead(ctx, ds, addr)
	return err
}

// HandleError implements CommitHook
func (ph *PushOnWriteHook) HandleError(ctx context.Context, err error) error {
	if ph.out != nil {
		_, err := ph.out.Write([]byte(fmt.Sprintf("error pushing: %+v", err)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (*PushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

// SetLogger implements CommitHook
func (ph *PushOnWriteHook) SetLogger(ctx context.Context, wr io.Writer) error {
	ph.out = wr
	return nil
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
	asyncPushBufferSize    = 2048
	asyncPushInterval      = 500 * time.Millisecond
	asyncPushProcessCommit = "async_push_process_commit"
	asyncPushSyncReplica   = "async_push_sync_replica"
)

var _ CommitHook = (*AsyncPushOnWriteHook)(nil)

// NewAsyncPushOnWriteHook creates a AsyncReplicateHook
func NewAsyncPushOnWriteHook(bThreads *sql.BackgroundThreads, destDB *DoltDB, tmpDir string, logger io.Writer) (*AsyncPushOnWriteHook, error) {
	ch := make(chan PushArg, asyncPushBufferSize)
	err := RunAsyncReplicationThreads(bThreads, ch, destDB, tmpDir, logger)
	if err != nil {
		return nil, err
	}
	return &AsyncPushOnWriteHook{ch: ch}, nil
}

func (*AsyncPushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ah *AsyncPushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	addr, _ := ds.MaybeHeadAddr()

	select {
	case ah.ch <- PushArg{ds: ds, db: db, hash: addr}:
	case <-ctx.Done():
		ah.ch <- PushArg{ds: ds, db: db, hash: addr}
		return ctx.Err()
	}
	return nil
}

// HandleError implements CommitHook
func (ah *AsyncPushOnWriteHook) HandleError(ctx context.Context, err error) error {
	if ah.out != nil {
		ah.out.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements CommitHook
func (ah *AsyncPushOnWriteHook) SetLogger(ctx context.Context, wr io.Writer) error {
	ah.out = wr
	return nil
}

type LogHook struct {
	msg []byte
	out io.Writer
}

var _ CommitHook = (*LogHook)(nil)

// NewLogHook is a noop that logs to a writer when invoked
func NewLogHook(msg []byte) *LogHook {
	return &LogHook{msg: msg}
}

// Execute implements CommitHook, writes message to log channel
func (lh *LogHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	if lh.out != nil {
		_, err := lh.out.Write(lh.msg)
		return err
	}
	return nil
}

// HandleError implements CommitHook
func (lh *LogHook) HandleError(ctx context.Context, err error) error {
	if lh.out != nil {
		lh.out.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements CommitHook
func (lh *LogHook) SetLogger(ctx context.Context, wr io.Writer) error {
	lh.out = wr
	return nil
}

func (*LogHook) ExecuteForWorkingSets() bool {
	return false
}

func RunAsyncReplicationThreads(bThreads *sql.BackgroundThreads, ch chan PushArg, destDB *DoltDB, tmpDir string, logger io.Writer) error {
	mu := &sync.Mutex{}
	var newHeads = make(map[string]PushArg, asyncPushBufferSize)

	updateHead := func(p PushArg) {
		mu.Lock()
		newHeads[p.ds.ID()] = p
		mu.Unlock()
	}

	// newCtx lets first goroutine drain before the second goroutine finalizes
	newCtx, stop := context.WithCancel(context.Background())

	// The first goroutine amortizes commits into a map keyed by dataset id.
	// When the parent context cancels, this goroutine drains and kills its
	// dependent goroutine.
	//
	// We do not track sequential commits because push follows historical
	// dependencies. This does not account for reset --force, which
	// breaks historical dependence.
	err := bThreads.Add(asyncPushProcessCommit, func(ctx context.Context) {
		for {
			select {
			case p, ok := <-ch:
				if !ok {
					return
				}
				updateHead(p)
			case <-ctx.Done():
				stop()
				return
			}
		}
	})
	if err != nil {
		return err
	}

	getHeadsCopy := func() map[string]PushArg {
		mu.Lock()
		defer mu.Unlock()
		if len(newHeads) == 0 {
			return nil
		}

		var newHeadsCopy = make(map[string]PushArg, asyncPushBufferSize)
		for k, v := range newHeads {
			newHeadsCopy[k] = v
		}

		return newHeadsCopy
	}

	flush := func(newHeads map[string]PushArg, latestHeads map[string]hash.Hash) {
		newHeadsCopy := getHeadsCopy()
		if len(newHeadsCopy) == 0 {
			return
		}
		for id, newCm := range newHeadsCopy {
			if latest, ok := latestHeads[id]; !ok || latest != newCm.hash {
				// use background context to drain after sql context is canceled
				err := pushDataset(context.Background(), destDB.db, newCm.db, newCm.ds, tmpDir)
				if err != nil {
					logger.Write([]byte("replication failed: " + err.Error()))
				}
				if newCm.hash.IsEmpty() {
					delete(latestHeads, id)
				} else {
					latestHeads[id] = newCm.hash
				}
			}
		}
	}

	// The second goroutine pushes updates to a remote chunkstore.
	// This goroutine waits for first goroutine to drain before closing
	// the channel and exiting.
	err = bThreads.Add(asyncPushSyncReplica, func(ctx context.Context) {
		defer close(ch)
		var latestHeads = make(map[string]hash.Hash, asyncPushBufferSize)
		ticker := time.NewTicker(asyncPushInterval)
		for {
			select {
			case <-newCtx.Done():
				flush(newHeads, latestHeads)
				return
			case <-ticker.C:
				flush(newHeads, latestHeads)
			}
		}
	})
	if err != nil {
		return err
	}

	return nil
}
