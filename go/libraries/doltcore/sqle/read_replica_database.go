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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
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

	return ReadReplicaDatabase{
		Database: db,
		remote:   remote,
		tmpDir:   dEnv.TempTableFilesDir(),
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
	_, headsArg, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateHeadsKey)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(dsess.ReplicateHeadsKey)
	}

	_, allHeads, ok := sql.SystemVariables.GetGlobal(dsess.ReplicateAllHeadsKey)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(dsess.ReplicateAllHeadsKey)
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	currentBranchRef, err := dSess.CWBHeadRef(ctx, rrd.name)
	if err != nil {
		return err
	}
	switch {
	case headsArg != "" && allHeads == SysVarTrue:
		return fmt.Errorf("%w; cannot set both 'dolt_replicate_heads' and 'dolt_replicate_all_heads'", ErrInvalidReplicateHeadsSetting)
	case headsArg != "":
		heads, ok := headsArg.(string)
		if !ok {
			return sql.ErrInvalidSystemVariableValue.New(dsess.ReplicateHeadsKey)
		}
		branches := parseBranches(heads)
		err := rrd.srcDB.Rebase(ctx)
		if err != nil {
			return err
		}
		err = pullBranches(ctx, rrd, branches, currentBranchRef)
		if err != nil {
			return err
		}
	case allHeads == int8(1):
		err := rrd.srcDB.Rebase(ctx)
		if err != nil {
			return err
		}
		toPull, toDelete, err := getReplicationBranches(ctx, rrd)
		err = pullBranches(ctx, rrd, toPull, currentBranchRef)
		if err != nil {
			return err
		}
		err = deleteBranches(ctx, rrd, toDelete, currentBranchRef)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: dolt_replicate_heads not set", ErrInvalidReplicateHeadsSetting)
	}
	return nil
}

func pullBranches(ctx *sql.Context, rrd ReadReplicaDatabase, branches []string, currentBranchRef ref.DoltRef) error {
	refSpecs, err := env.ParseRSFromArgs(rrd.remote.Name, branches)
	if err != nil {
		return err
	}

	for i, refSpec := range refSpecs {
		branch := ref.NewBranchRef(branches[i])
		rtRef := refSpec.DestRef(branch)

		fetchCtx := ctx.Context

		srcDBCommitA, err := rrd.limiter.Run(ctx, branch.String(), func() (any, error) {
			// fetch remote tracking ref only
			var srcDBCommit *doltdb.Commit
			{
				srcDBCommit, err = actions.FetchRemoteBranch(fetchCtx, rrd.tmpDir, rrd.remote, rrd.srcDB, rrd.ddb, branch, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
				if err != nil {
					return nil, err
				}

				err = rrd.ddb.FastForward(fetchCtx, rtRef, srcDBCommit)
				if err != nil {
					return nil, err
				}
			}

			// either ff or create local tracking branch
			{
				branchExists, err := rrd.ddb.HasBranch(fetchCtx, branch.GetPath())
				switch {
				case err != nil:
				case branchExists:
					err = rrd.ddb.FastForward(fetchCtx, branch, srcDBCommit)
				default:
					err = rrd.ddb.NewBranchAtCommit(fetchCtx, branch, srcDBCommit)
				}
				if err != nil {
					return nil, err
				}
			}
			return srcDBCommit, nil
		})
		if err != nil {
			return err
		}

		srcDBCommit := srcDBCommitA.(*doltdb.Commit)

		// if ref is active head, update the current working set
		{
			if branch == currentBranchRef {
				wsRef, err := ref.WorkingSetRefForHead(branch)
				if err != nil {
					return err
				}

				ws, err := rrd.ddb.ResolveWorkingSet(ctx, wsRef)
				if err != nil {
					return err
				}

				commitRoot, err := srcDBCommit.GetRootValue(ctx)
				if err != nil {
					return err
				}

				ws = ws.WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)
				h, err := ws.HashOf()
				if err != nil {
					return err
				}

				rrd.ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, h, doltdb.TodoWorkingSetMeta())
			}
		}
	}

	_, err = rrd.limiter.Run(ctx, "___tags", func() (any, error) {
		// TODO: Not sure about this; see comment about the captured ctx below.

		return nil, actions.FetchFollowTags(ctx, rrd.rsw.TempTableFilesDir(), rrd.srcDB, rrd.ddb, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	})
	if err != nil {
		return err
	}

	return nil
}

func getReplicationBranches(ctx *sql.Context, rrd ReadReplicaDatabase) (allBranches []string, deletedBranches []ref.DoltRef, err error) {
	remRefs, err := rrd.srcDB.GetBranches(ctx)
	if err != nil {
		return nil, nil, err
	}

	localRefs, err := rrd.Database.ddb.GetBranches(ctx)
	if err != nil {
		return nil, nil, err
	}

	deletedBranches = branchesToDelete(remRefs, localRefs)
	allBranches = make([]string, len(remRefs))
	for i := range remRefs {
		allBranches[i] = remRefs[i].GetPath()
	}

	return allBranches, deletedBranches, nil
}

func branchesToDelete(remRefs, localRefs []ref.DoltRef) []ref.DoltRef {
	toDelete := make([]ref.DoltRef, 0, len(localRefs))
	var i, j int
	for i < len(remRefs) && j < len(localRefs) {
		rem := remRefs[i].GetPath()
		local := localRefs[j].GetPath()
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

func deleteBranches(ctx *sql.Context, rrd ReadReplicaDatabase, branches []ref.DoltRef, currentBranchRef ref.DoltRef) error {
	for _, b := range branches {
		err := rrd.ddb.DeleteBranch(ctx, b)
		if errors.Is(err, doltdb.ErrBranchNotFound) {
			continue
		} else if err != nil {
			return err
		}
	}
	return nil
}

func parseBranches(arg string) []string {
	heads := strings.Split(arg, ",")
	branches := make([]string, 0, len(heads))
	for _, head := range heads {
		branches = append(branches, head)
	}
	return branches
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
