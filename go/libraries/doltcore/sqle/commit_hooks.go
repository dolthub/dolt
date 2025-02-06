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

package sqle

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

type PushOnWriteHook struct {
	destDB *doltdb.DoltDB
	tmpDir string
	out    io.Writer
}

var _ doltdb.CommitHook = (*PushOnWriteHook)(nil)

// NewPushOnWriteHook creates a ReplicateHook, parameterizaed by the backup database
// and a local tempfile for pushing
func NewPushOnWriteHook(destDB *doltdb.DoltDB, tmpDir string) *PushOnWriteHook {
	return &PushOnWriteHook{
		destDB: destDB,
		tmpDir: tmpDir,
	}
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ph *PushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	return nil, pushDataset(ctx, ph.destDB, db, ds, ph.tmpDir)
}

func pushDataset(ctx context.Context, destDB, srcDB *doltdb.DoltDB, ds datas.Dataset, tmpDir string) error {
	addr, ok := ds.MaybeHeadAddr()
	if !ok {
		// TODO: fix up hack usage.
		_, err := doltdb.HackDatasDatabaseFromDoltDB(destDB).Delete(ctx, ds, "")
		return err
	}

	err := destDB.PullChunks(ctx, tmpDir, srcDB, []hash.Hash{addr}, nil, nil)
	if err != nil {
		return err
	}

	rf, err := ref.Parse(ds.ID())
	if err != nil {
		return err
	}

	return destDB.SetHead(ctx, rf, addr)
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
	db   *doltdb.DoltDB
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

var _ doltdb.CommitHook = (*AsyncPushOnWriteHook)(nil)

// NewAsyncPushOnWriteHook creates a AsyncReplicateHook
func NewAsyncPushOnWriteHook(destDB *doltdb.DoltDB, tmpDir string, logger io.Writer) (*AsyncPushOnWriteHook, RunAsyncThreads) {
	ch := make(chan PushArg, asyncPushBufferSize)
	runThreads := func(bThreads *sql.BackgroundThreads, ctxF func(context.Context) (*sql.Context, error)) error {
		return RunAsyncReplicationThreads(bThreads, ch, destDB, tmpDir, logger)
	}
	return &AsyncPushOnWriteHook{ch: ch}, runThreads
}

func (*AsyncPushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ah *AsyncPushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	addr, _ := ds.MaybeHeadAddr()
	// TODO: Unconditional push here seems dangerous.
	ah.ch <- PushArg{ds: ds, db: db, hash: addr}
	return nil, ctx.Err()
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

var _ doltdb.CommitHook = (*LogHook)(nil)

// NewLogHook is a noop that logs to a writer when invoked
func NewLogHook(msg []byte) *LogHook {
	return &LogHook{msg: msg}
}

// Execute implements CommitHook, writes message to log channel
func (lh *LogHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	if lh.out != nil {
		_, err := lh.out.Write(lh.msg)
		return nil, err
	}
	return nil, nil
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

func RunAsyncReplicationThreads(bThreads *sql.BackgroundThreads, ch chan PushArg, destDB *doltdb.DoltDB, tmpDir string, logger io.Writer) error {
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

		toRet := newHeads
		newHeads = make(map[string]PushArg, asyncPushBufferSize)

		return toRet
	}

	flush := func(newHeads map[string]PushArg, latestHeads map[string]hash.Hash) {
		newHeadsCopy := getHeadsCopy()
		if len(newHeadsCopy) == 0 {
			return
		}
		for id, newCm := range newHeadsCopy {
			if latest, ok := latestHeads[id]; !ok || latest != newCm.hash {
				// use background context to drain after sql context is canceled
				err := pushDataset(context.Background(), destDB, newCm.db, newCm.ds, tmpDir)
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
