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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

type ReadReplicaDatabase struct {
	Database
	remote  env.Remote
	srcDB   *doltdb.DoltDB
	tmpDir  string
	limiter *limiter
}

var _ SqlDatabase = ReadReplicaDatabase{}
var _ sql.VersionedDatabase = ReadReplicaDatabase{}
var _ sql.TableDropper = ReadReplicaDatabase{}
var _ sql.TableCreator = ReadReplicaDatabase{}
var _ sql.TemporaryTableCreator = ReadReplicaDatabase{}
var _ sql.TableRenamer = ReadReplicaDatabase{}
var _ sql.TriggerDatabase = &ReadReplicaDatabase{}
var _ sql.StoredProcedureDatabase = ReadReplicaDatabase{}
var _ sql.TransactionDatabase = ReadReplicaDatabase{}

var ErrFailedToLoadReplicaDB = errors.New("failed to load replica database")
var ErrInvalidReplicateHeadsSetting = errors.New("invalid replicate heads setting")
var ErrFailedToCastToReplicaDb = errors.New("failed to cast to ReadReplicaDatabase")
var ErrCannotCreateReplicaRevisionDbForCommit = errors.New("cannot create replica revision db for commit")

var EmptyReadReplica = ReadReplicaDatabase{}

func NewReadReplicaDatabase(ctx context.Context, db Database, remoteName string, dEnv *env.DoltEnv) (ReadReplicaDatabase, error) {
	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return EmptyReadReplica, err
	}

	remote, ok := remotes[remoteName]
	if !ok {
		return EmptyReadReplica, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	srcDB, err := remote.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return EmptyReadReplica, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return EmptyReadReplica, err
	}

	return ReadReplicaDatabase{
		Database: db,
		remote:   remote,
		tmpDir:   tmpDir,
		srcDB:    srcDB,
		limiter:  newLimiter(),
	}, nil
}

func (rrd ReadReplicaDatabase) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	if rrd.srcDB != nil {
		err := rrd.PullFromRemote(ctx)
		if err != nil {
			err = fmt.Errorf("replication failed: %w", err)
			if !SkipReplicationWarnings() {
				return nil, err
			}
			ctx.GetLogger().Warn(err.Error())
		}
	} else {
		ctx.GetLogger().Warn("replication failed; dolt_replication_remote value is misconfigured")
	}
	return rrd.Database.StartTransaction(ctx, tCharacteristic)
}

func (rrd ReadReplicaDatabase) PullFromRemote(ctx *sql.Context) error {
	_, headsArg, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateHeads)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(dsess.ReplicateHeads)
	}

	_, allHeads, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateAllHeads)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(dsess.ReplicateAllHeads)
	}

	behavior := pullBehavior_fastForward
	if ReadReplicaForcePull() {
		behavior = pullBehavior_forcePull
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	currentBranchRef, err := dSess.CWBHeadRef(ctx, rrd.name)
	if err != nil && !SkipReplicationWarnings() {
		return err
	} else if err != nil {
		ctx.GetLogger().Warn(err.Error())
		return nil
	}

	err = rrd.srcDB.Rebase(ctx)
	if err != nil && !SkipReplicationWarnings() {
		return err
	} else if err != nil {
		ctx.GetLogger().Warn(err.Error())
		return nil
	}

	remoteBranches, localBranches, toDelete, err := getReplicationBranches(ctx, rrd)
	if err != nil && !SkipReplicationWarnings() {
		return err
	} else if err != nil {
		ctx.GetLogger().Warn(err.Error())
		return nil
	}

	switch {
	case headsArg != "" && allHeads == SysVarTrue:
		return fmt.Errorf("%w; cannot set both 'dolt_replicate_heads' and 'dolt_replicate_all_heads'", ErrInvalidReplicateHeadsSetting)
	case headsArg != "":
		heads, ok := headsArg.(string)
		if !ok {
			return sql.ErrInvalidSystemVariableValue.New(dsess.ReplicateHeads)
		}
		branches := strings.Split(heads, ",")
		branchesToPull := make(map[string]bool)
		for _, branch := range branches {
			branchesToPull[branch] = true
		}

		// Reduce the remote branch list to only the ones configured to replicate
		prunedBranches := make([]doltdb.BranchWithHash, len(branchesToPull))
		pruneI := 0
		for _, remoteBranch := range remoteBranches {
			if branchesToPull[remoteBranch.Ref.GetPath()] {
				prunedBranches[pruneI] = remoteBranch
				pruneI++
			}
			delete(branchesToPull, remoteBranch.Ref.GetPath())
		}

		if len(branchesToPull) > 0 {
			// just use the first not-found branch as the error string
			var branch string
			for b := range branchesToPull {
				branch = b
				break
			}

			err := fmt.Errorf("unable to find %q on %q; branch not found", branch, rrd.remote.Name)
			if err != nil && !SkipReplicationWarnings() {
				return err
			} else if err != nil {
				ctx.GetLogger().Warn(err.Error())
				return nil
			}
		}

		remoteBranches = prunedBranches
		err = pullBranches(ctx, rrd, remoteBranches, localBranches, currentBranchRef, behavior)

		if err != nil && !SkipReplicationWarnings() {
			return err
		} else if err != nil {
			ctx.GetLogger().Warn(err.Error())
			return nil
		}

	case allHeads == int8(1):
		err = pullBranches(ctx, rrd, remoteBranches, localBranches, currentBranchRef, behavior)
		if err != nil && !SkipReplicationWarnings() {
			return err
		} else if err != nil {
			ctx.GetLogger().Warn(err.Error())
			return nil
		}

		err = deleteBranches(ctx, rrd, toDelete)
		if err != nil && !SkipReplicationWarnings() {
			return err
		} else if err != nil {
			ctx.GetLogger().Warn(err.Error())
			return nil
		}
	default:
		return fmt.Errorf("%w: dolt_replicate_heads not set", ErrInvalidReplicateHeadsSetting)
	}

	return nil
}

func (rrd ReadReplicaDatabase) RebaseSourceDb(ctx *sql.Context) error {
	return rrd.srcDB.Rebase(ctx)
}

type pullBehavior bool

const pullBehavior_fastForward pullBehavior = false
const pullBehavior_forcePull pullBehavior = true

// pullBranches pulls the remote branches named. If a corresponding local branch exists, it will be fast-forwarded. If
// it doesn't exist, it will be created.
func pullBranches(
	ctx *sql.Context,
	rrd ReadReplicaDatabase,
	remoteBranches []doltdb.BranchWithHash,
	localBranches []doltdb.BranchWithHash,
	currentBranchRef ref.DoltRef,
	behavior pullBehavior,
) error {
	localHashesByRef := make(map[string]hash.Hash)
	remoteHashesByRef := make(map[string]hash.Hash)

	for _, b := range remoteBranches {
		remoteHashesByRef[b.Ref.GetPath()] = b.Hash
	}

	for _, b := range localBranches {
		localHashesByRef[b.Ref.GetPath()] = b.Hash
	}

	_, err := rrd.limiter.Run(ctx, "-all", func() (any, error) {
		srcRoot, err := rrd.srcDB.NomsRoot(ctx)
		if err != nil {
			return nil, err
		}

		err = rrd.ddb.PullChunks(ctx, rrd.tmpDir, rrd.srcDB, srcRoot, nil, nil)

		for _, br := range remoteBranches {
			_, branchExists := localHashesByRef[br.Ref.GetPath()]
			switch {
			case err != nil:
			case branchExists:
				if behavior == pullBehavior_forcePull {
					err = rrd.ddb.SetHead(ctx, br.Ref, br.Hash)
					if err != nil {
						return nil, err
					}
				} else if localHashesByRef[br.Ref.GetPath()] != br.Hash {
					err = rrd.ddb.FastForwardToHash(ctx, br.Ref, br.Hash)
					if err != nil {
						return nil, err
					}
				}
			default:
				cm, err := rrd.srcDB.ReadCommit(ctx, br.Hash)
				if err != nil {
					return nil, err
				}

				err = rrd.ddb.NewBranchAtCommit(ctx, br.Ref, cm)
				if err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	// update the current working set if necessary
	if h, ok := remoteHashesByRef[currentBranchRef.GetPath()]; ok {
		cm, err := rrd.srcDB.ReadCommit(ctx, h)
		wsRef, err := ref.WorkingSetRefForHead(currentBranchRef)
		if err != nil {
			return err
		}

		ws, err := rrd.ddb.ResolveWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}

		commitRoot, err := cm.GetRootValue(ctx)
		if err != nil {
			return err
		}

		ws = ws.WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)
		h, err := ws.HashOf()
		if err != nil {
			return err
		}

		return rrd.ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, h, doltdb.TodoWorkingSetMeta())
	}

	_, err = rrd.limiter.Run(ctx, "___tags", func() (any, error) {
		tmpDir, err := rrd.rsw.TempTableFilesDir()
		if err != nil {
			return nil, err
		}
		// TODO: Not sure about this; see comment about the captured ctx below.
		return nil, actions.FetchFollowTags(ctx, tmpDir, rrd.srcDB, rrd.ddb, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	})
	if err != nil {
		return err
	}

	return nil
}

func getReplicationBranches(ctx *sql.Context, rrd ReadReplicaDatabase) (
	remoteBranches []doltdb.BranchWithHash,
	localBranches []doltdb.BranchWithHash,
	deletedBranches []doltdb.BranchWithHash,
	err error,
) {
	remoteBranches, err = rrd.srcDB.GetBranchesWithHashes(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	localBranches, err = rrd.Database.ddb.GetBranchesWithHashes(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	deletedBranches = branchesToDelete(remoteBranches, localBranches)
	return remoteBranches, localBranches, deletedBranches, nil
}

func branchesToDelete(remRefs, localRefs []doltdb.BranchWithHash) []doltdb.BranchWithHash {
	toDelete := make([]doltdb.BranchWithHash, 0, len(localRefs))
	var i, j int
	for i < len(remRefs) && j < len(localRefs) {
		rem := remRefs[i].Ref.GetPath()
		local := localRefs[j].Ref.GetPath()
		if rem == local {
			i++
			j++
		} else if rem < local {
			i++
		} else {
			toDelete = append(toDelete, localRefs[j])
			j++
		}
	}
	for j < len(localRefs) {
		toDelete = append(toDelete, localRefs[j])
		j++
	}
	return toDelete
}

func deleteBranches(ctx *sql.Context, rrd ReadReplicaDatabase, branches []doltdb.BranchWithHash) error {
	for _, b := range branches {
		err := rrd.ddb.DeleteBranch(ctx, b.Ref)
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			continue
		} else if err != nil {
			return err
		}
	}
	return nil
}

type res struct {
	v   any
	err error
}

type blocked struct {
	f       func() (any, error)
	waiters []chan res
}

func newLimiter() *limiter {
	return &limiter{
		running: make(map[string]*blocked),
	}
}

// *limiter allows a caller to limit performing concurrent work for a given string key.
type limiter struct {
	mu      sync.Mutex
	running map[string]*blocked
}

// |Run| invokes |f|, returning its result. It does not allow two |f|s
// submitted with the same |s| to be running in concurrently.
// Only one of the |f|s that arrives with the same |s| while another |f| with
// that key is running will ultimately be run. The result of invoking that |f|
// will be returned from the |Run| call to all blockers on that key.
//
// 1) A caller provides a string key, |s|, and an |f func() error| which will
// perform the work when invoked.
//
// 2) If there is no outstanding call for the key, |f| is invoked and the
// result is returned.
//
// 3) Otherwise, the caller blocks until the outstanding call is completed.
// When the outstanding call completes, one of the blocked |f|s that was
// provided for that key is run. The result of that invocation is returned to
// all blocked callers.
//
// A caller's |Run| invocation can return early if the context is cancelled.
// If the |f| captures a context, and that context is canceled, and the |f|
// allows the error from that context cancelation to escape, then multiple
// callers will see ContextCanceled / DeadlineExceeded, even if their contexts
// are not canceled.
//
// This implementation is very naive and is not not optimized for high
// contention on |l.running|/|l.mu|.
func (l *limiter) Run(ctx context.Context, s string, f func() (any, error)) (any, error) {
	l.mu.Lock()
	if b, ok := l.running[s]; ok {
		// Something is already running; add ourselves to waiters.
		ch := make(chan res)
		if b.f == nil {
			// We are the first waiter; we set what |f| will be invoked.
			b.f = f
		}
		b.waiters = append(b.waiters, ch)
		l.mu.Unlock()
		select {
		case r := <-ch:
			return r.v, r.err
		case <-ctx.Done():
			go func() { <-ch }()
			return nil, ctx.Err()
		}
	} else {
		// We can run immediately and return the result of |f|.
		// Register ourselves as running.
		l.running[s] = new(blocked)
		l.mu.Unlock()
	}

	res, err := f()
	l.finish(s)
	return res, err
}

// Called anytime work is finished on a given key. Responsible for
// starting any blocked work on |s| and delivering the results to waiters.
func (l *limiter) finish(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.running[s]
	if len(b.waiters) != 0 {
		go func() {
			r, err := b.f()
			for _, ch := range b.waiters {
				ch <- res{r, err}
				close(ch)
			}
			l.finish(s)
		}()
		// Just started work for the existing |*blocked|, make a new
		// |*blocked| for work that arrives from this point forward.
		l.running[s] = new(blocked)
	} else {
		// No work is pending. Delete l.running[s] since nothing is
		// running anymore.
		delete(l.running, s)
	}
}
