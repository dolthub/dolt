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

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

type PushOnWriteHook struct {
	out    io.Writer
	destDB *doltdb.DoltDB
	tmpDir string
}

var _ doltdb.CommitHook = (*PushOnWriteHook)(nil)

// NewPushOnWriteHook creates a ReplicateHook, parameterizaed by the backup database
// and a local tempfile for pushing
func NewPushOnWriteHook(tmpDir string, logger io.Writer) *PushOnWriteHook {
	return &PushOnWriteHook{
		tmpDir: tmpDir,
		out:    logger,
	}
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ph *PushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, srcDb *doltdb.DoltDB) (func(context.Context) error, error) {
	if ph.destDB == nil {
		panic("NM4")
	}

	err := pushDataset(ctx, ph.destDB, srcDb, ds, ph.tmpDir)

	if ph.out != nil && err != nil {
		// if we can't write to the output, there's not much we can do.
		_, _ = ph.out.Write([]byte(fmt.Sprintf("error pushing: %+v", err)))
	}

	return nil, err
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

func (*PushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

type PushArg struct {
	ds     datas.Dataset
	srcDb  *doltdb.DoltDB
	destDb *doltdb.DoltDB
	hash   hash.Hash
}

type AsyncPushOnWriteHook struct {
	out io.Writer
	ch  chan PushArg

	destDb *doltdb.DoltDB
}

const (
	asyncPushBufferSize    = 2048
	asyncPushInterval      = 500 * time.Millisecond
	asyncPushProcessCommit = "async_push_process_commit"
	asyncPushSyncReplica   = "async_push_sync_replica"
)

var _ doltdb.CommitHook = (*AsyncPushOnWriteHook)(nil)

// NewAsyncPushOnWriteHook creates a AsyncReplicateHook
func NewAsyncPushOnWriteHook(tmpDir string, logger io.Writer) (*AsyncPushOnWriteHook, RunAsyncThreads) {
	ch := make(chan PushArg, asyncPushBufferSize)
	runThreads := func(bThreads *sql.BackgroundThreads, ctxF func(context.Context) (*sql.Context, error)) error {
		return RunAsyncReplicationThreads(bThreads, ctxF, ch, tmpDir, logger)
	}
	return &AsyncPushOnWriteHook{ch: ch}, runThreads
}

func (*AsyncPushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

// Execute implements CommitHook, replicates head updates to the destDb field
func (ah *AsyncPushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, srcDb *doltdb.DoltDB) (func(context.Context) error, error) {
	if ah.destDb == nil {
		panic("NM4")
	}

	addr, _ := ds.MaybeHeadAddr()
	// TODO: Unconditional push here seems dangerous.
	ah.ch <- PushArg{ds: ds, srcDb: srcDb, destDb: ah.destDb, hash: addr}

	err := ctx.Err()
	if err != nil {
		_, _ = ah.out.Write([]byte(err.Error()))
	}

	return nil, err
}

type LogHook struct {
	out io.Writer
	msg []byte
}

var _ doltdb.CommitHook = (*LogHook)(nil)

// NewLogHook is a noop that logs to a writer when invoked
func NewLogHook(msg []byte, logger io.Writer) *LogHook {
	return &LogHook{msg: msg, out: logger}
}

// Execute implements CommitHook, writes message to log channel
func (lh *LogHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	if lh.out != nil {
		_, _ = lh.out.Write(lh.msg)
	}
	return nil, nil
}

func (*LogHook) ExecuteForWorkingSets() bool {
	return false
}

func RunAsyncReplicationThreads(bThreads *sql.BackgroundThreads, ctxF func(context.Context) (*sql.Context, error), ch chan PushArg, tmpDir string, logger io.Writer) error {
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
				func() {
					// use background context to drain after sql context is canceled
					sqlCtx, err := ctxF(context.Background())
					if err != nil {
						logger.Write([]byte("replication failed: could not create *sql.Context: " + err.Error()))
					} else {
						defer sql.SessionEnd(sqlCtx.Session)
						sql.SessionCommandBegin(sqlCtx.Session)
						defer sql.SessionCommandEnd(sqlCtx.Session)
						err := pushDataset(sqlCtx, newCm.destDb, newCm.srcDb, newCm.ds, tmpDir)
						if err != nil {
							logger.Write([]byte("replication failed: " + err.Error()))
						}
						if newCm.hash.IsEmpty() {
							delete(latestHeads, id)
						} else {
							latestHeads[id] = newCm.hash
						}
					}
				}()
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

type DynamicPushOnWriteHook struct {
	mu      sync.Mutex
	dEnv    *env.DoltEnv
	tempDir string
	logger  io.Writer

	// Values below protected with mu
	remote    string
	async     bool
	syncHook  PushOnWriteHook
	asyncHook AsyncPushOnWriteHook
}

func NewDynamicPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv, logger io.Writer) (*DynamicPushOnWriteHook, RunAsyncThreads, error) {
	remote, async, err := getReplicationVals()
	if err != nil {
		return nil, nil, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, nil, err
	}

	a, newThreads := NewAsyncPushOnWriteHook(tmpDir, logger)
	p := NewPushOnWriteHook(tmpDir, logger)

	if remote != "" {
		destDb, err := getDestinationDb(ctx, dEnv, remote)
		if err != nil {
			return nil, nil, err
		}
		p.destDB = destDb
		a.destDb = destDb
	}

	return &DynamicPushOnWriteHook{
		dEnv:      dEnv,
		tempDir:   tmpDir,
		logger:    logger,
		remote:    remote,
		async:     async,
		syncHook:  *p,
		asyncHook: *a,
	}, newThreads, nil
}

func getReplicationVals() (string, bool, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateToRemote)
	if !ok {
		return "", false, sql.ErrUnknownSystemVariable.New(dsess.ReplicateToRemote)
	}
	remoteName, ok := val.(string)
	if !ok {
		return "", false, sql.ErrInvalidSystemVariableValue.New(val)
	}

	async := false
	if _, val, ok = sql.SystemVariables.GetGlobal(dsess.AsyncReplication); ok && val == dsess.SysVarTrue {
		async = true
	}

	return remoteName, async, nil
}

func getDestinationDb(ctx context.Context, dEnv *env.DoltEnv, remoteName string) (*doltdb.DoltDB, error) {
	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes.Get(remoteName)
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	destDb, err := rem.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return nil, err
	}
	return destDb, nil
}

func (m *DynamicPushOnWriteHook) Execute(ctx context.Context, ds datas.Dataset, db *doltdb.DoltDB) (func(context.Context) error, error) {
	remoteName, async, err := getReplicationVals()

	// We only need the lock if the remote configuration has changed.
	hook, err := func() (doltdb.CommitHook, error) {
		m.mu.Lock()
		defer m.mu.Unlock()

		if m.remote == remoteName && m.async == async {
			// No change in config since last execution.
			if m.remote == "" {
				// replication disabled
				return nil, nil
			}

			if async {
				return &m.asyncHook, nil
			}
			return &m.syncHook, nil
		}

		if remoteName == "" {
			// replication disabled
			m.remote = ""
			m.async = false
			m.asyncHook.destDb = nil
			m.syncHook.destDB = nil
			return nil, nil
		}

		m.remote = remoteName

		destDb, err := getDestinationDb(ctx, m.dEnv, m.remote)
		if err != nil {
			return nil, err
		}

		m.syncHook.destDB = destDb
		m.asyncHook.destDb = destDb

		if async {
			return &m.asyncHook, nil
		}
		return &m.syncHook, nil
	}()

	if err != nil {
		logrus.Warnf("replication hook failed: %v", err)
		return nil, err
	}

	if hook == nil {
		// replication disabled
		return nil, nil
	}

	logrus.Infof("replication hook invoked. pushing to '%s' (asyn=%t)", remoteName, async)
	return hook.Execute(ctx, ds, db)
}

func (m *DynamicPushOnWriteHook) ExecuteForWorkingSets() bool {
	return false
}

var _ doltdb.CommitHook = (*DynamicPushOnWriteHook)(nil)
