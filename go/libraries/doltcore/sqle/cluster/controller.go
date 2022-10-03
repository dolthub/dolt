// Copyright 2022 Dolthub, Inc.
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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/clusterdb"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

type Role string

const RolePrimary Role = "primary"
const RoleStandby Role = "standby"
const RoleDetectedBrokenConfig Role = "detected_broken_config"

const PersistentConfigPrefix = "sqlserver.cluster"

type Controller struct {
	cfg           Config
	persistentCfg config.ReadWriteConfig
	role          Role
	epoch         int
	systemVars    sqlvars
	mu            sync.Mutex
	commithooks   []*commithook
	sinterceptor  serverinterceptor
	cinterceptor  clientinterceptor
	lgr           *logrus.Logger

	provider       dbProvider
	iterSessions   IterSessions
	killQuery      func(uint32)
	killConnection func(uint32) error
}

type sqlvars interface {
	AddSystemVariables(sysVars []sql.SystemVariable)
}

// We can manage certain aspects of the exposed databases on the server through
// this.
type dbProvider interface {
	SetIsStandby(bool)
}

type procedurestore interface {
	Register(sql.ExternalStoredProcedureDetails)
}

const (
	DoltClusterRoleVariable      = "dolt_cluster_role"
	DoltClusterRoleEpochVariable = "dolt_cluster_role_epoch"
)

func NewController(lgr *logrus.Logger, cfg Config, pCfg config.ReadWriteConfig) (*Controller, error) {
	if cfg == nil {
		return nil, nil
	}
	pCfg = config.NewPrefixConfig(pCfg, PersistentConfigPrefix)
	role, epoch, err := applyBootstrapClusterConfig(lgr, cfg, pCfg)
	if err != nil {
		return nil, err
	}
	ret := &Controller{
		cfg:           cfg,
		persistentCfg: pCfg,
		role:          role,
		epoch:         epoch,
		commithooks:   make([]*commithook, 0),
		lgr:           lgr,
	}
	roleSetter := func(role string, epoch int) {
		ret.setRoleAndEpoch(role, epoch, false /* graceful */, -1 /* saveConnID */)
	}
	ret.sinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.sinterceptor.setRole(role, epoch)
	ret.sinterceptor.roleSetter = roleSetter
	ret.cinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.cinterceptor.setRole(role, epoch)
	ret.cinterceptor.roleSetter = roleSetter
	return ret, nil
}

func (c *Controller) ManageSystemVariables(variables sqlvars) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.systemVars = variables
	c.refreshSystemVars()
}

func (c *Controller) ApplyStandbyReplicationConfig(ctx context.Context, bt *sql.BackgroundThreads, mrEnv *env.MultiRepoEnv, dbs ...sqle.SqlDatabase) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, db := range dbs {
		denv := mrEnv.GetEnv(db.Name())
		if denv == nil {
			continue
		}
		c.lgr.Tracef("cluster/controller: applying commit hooks for %s with role %s", db.Name(), string(c.role))
		hooks, err := c.applyCommitHooks(ctx, db.Name(), bt, denv)
		if err != nil {
			return err
		}
		c.commithooks = append(c.commithooks, hooks...)
	}
	return nil
}

type IterSessions func(func(sql.Session) (bool, error)) error

func (c *Controller) ManageDatabaseProvider(p dbProvider) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider = p
	c.setProviderIsStandby(c.role != RolePrimary)
}

func (c *Controller) ManageQueryConnections(iterSessions IterSessions, killQuery func(uint32), killConnection func(uint32) error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.iterSessions = iterSessions
	c.killQuery = killQuery
	c.killConnection = killConnection
}

func (c *Controller) applyCommitHooks(ctx context.Context, name string, bt *sql.BackgroundThreads, denv *env.DoltEnv) ([]*commithook, error) {
	ttfdir, err := denv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	remotes, err := denv.GetRemotes()
	if err != nil {
		return nil, err
	}
	dialprovider := c.gRPCDialProvider(denv)
	var hooks []*commithook
	for _, r := range c.cfg.StandbyRemotes() {
		remote, ok := remotes[r.Name()]
		if !ok {
			return nil, fmt.Errorf("sqle: cluster: standby replication: destination remote %s does not exist on database %s", r.Name(), name)
		}
		commitHook := newCommitHook(c.lgr, r.Name(), name, c.role, func(ctx context.Context) (*doltdb.DoltDB, error) {
			return remote.GetRemoteDB(ctx, types.Format_Default, dialprovider)
		}, denv.DoltDB, ttfdir)
		denv.DoltDB.PrependCommitHook(ctx, commitHook)
		if err := commitHook.Run(bt); err != nil {
			return nil, err
		}
		hooks = append(hooks, commitHook)
	}
	return hooks, nil
}

func (c *Controller) gRPCDialProvider(denv *env.DoltEnv) dbfactory.GRPCDialProvider {
	return grpcDialProvider{env.NewGRPCDialProviderFromDoltEnv(denv), &c.cinterceptor}
}

func (c *Controller) RegisterStoredProcedures(store procedurestore) {
	if c == nil {
		return
	}
	store.Register(newAssumeRoleProcedure(c))
}

func (c *Controller) ClusterDatabase() sql.Database {
	if c == nil {
		return nil
	}
	return clusterdb.NewClusterDatabase(c)
}

func (c *Controller) RemoteSrvPort() int {
	if c == nil {
		return -1
	}
	return c.cfg.RemotesAPIConfig().Port()
}

func (c *Controller) ServerOptions() []grpc.ServerOption {
	return c.sinterceptor.Options()
}

func (c *Controller) refreshSystemVars() {
	role, epoch := string(c.role), c.epoch
	vars := []sql.SystemVariable{
		{
			Name:    DoltClusterRoleVariable,
			Dynamic: false,
			Scope:   sql.SystemVariableScope_Persist,
			Type:    sql.NewSystemStringType(DoltClusterRoleVariable),
			Default: role,
		},
		{
			Name:    DoltClusterRoleEpochVariable,
			Dynamic: false,
			Scope:   sql.SystemVariableScope_Persist,
			Type:    sql.NewSystemIntType(DoltClusterRoleEpochVariable, 0, 9223372036854775807, false),
			Default: epoch,
		},
	}
	c.systemVars.AddSystemVariables(vars)
}

func (c *Controller) persistVariables() error {
	toset := make(map[string]string)
	toset[DoltClusterRoleVariable] = string(c.role)
	toset[DoltClusterRoleEpochVariable] = strconv.Itoa(c.epoch)
	return c.persistentCfg.SetStrings(toset)
}

func applyBootstrapClusterConfig(lgr *logrus.Logger, cfg Config, pCfg config.ReadWriteConfig) (Role, int, error) {
	toset := make(map[string]string)
	persistentRole := pCfg.GetStringOrDefault(DoltClusterRoleVariable, "")
	var roleFromPersistentConfig bool
	persistentEpoch := pCfg.GetStringOrDefault(DoltClusterRoleEpochVariable, "")
	if persistentRole == "" {
		if cfg.BootstrapRole() != "" {
			lgr.Tracef("cluster/controller: persisted cluster role was empty, apply bootstrap_role %s", cfg.BootstrapRole())
			persistentRole = cfg.BootstrapRole()
		} else {
			lgr.Trace("cluster/controller: persisted cluster role was empty, bootstrap_role was empty: defaulted to primary")
			persistentRole = "primary"
		}
		toset[DoltClusterRoleVariable] = persistentRole
	} else {
		roleFromPersistentConfig = true
		lgr.Tracef("cluster/controller: persisted cluster role is %s", persistentRole)
	}
	if persistentEpoch == "" {
		persistentEpoch = strconv.Itoa(cfg.BootstrapEpoch())
		lgr.Tracef("cluster/controller: persisted cluster role epoch is empty, took boostrap_epoch: %s", persistentEpoch)
		toset[DoltClusterRoleEpochVariable] = persistentEpoch
	} else {
		lgr.Tracef("cluster/controller: persisted cluster role epoch is %s", persistentEpoch)
	}
	if persistentRole != string(RolePrimary) && persistentRole != string(RoleStandby) {
		isallowed := persistentRole == string(RoleDetectedBrokenConfig) && roleFromPersistentConfig
		if !isallowed {
			return "", 0, fmt.Errorf("persisted role %s.%s = %s must be \"primary\" or \"secondary\"", PersistentConfigPrefix, DoltClusterRoleVariable, persistentRole)
		}
	}
	epochi, err := strconv.Atoi(persistentEpoch)
	if err != nil {
		return "", 0, fmt.Errorf("persisted role epoch %s.%s = %s must be an integer", PersistentConfigPrefix, DoltClusterRoleEpochVariable, persistentEpoch)
	}
	if len(toset) > 0 {
		err := pCfg.SetStrings(toset)
		if err != nil {
			return "", 0, err
		}
	}
	return Role(persistentRole), epochi, nil
}

func (c *Controller) setRoleAndEpoch(role string, epoch int, graceful bool, saveConnID int) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch == c.epoch && role == string(c.role) {
		return false, nil
	}

	if role != string(RolePrimary) && role != string(RoleStandby) && role != string(RoleDetectedBrokenConfig) {
		return false, fmt.Errorf("error assuming role '%s'; valid roles are 'primary' and 'standby'", role)
	}

	if epoch < c.epoch {
		return false, fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d", role, epoch, c.epoch)
	}
	if epoch == c.epoch {
		// This is allowed for non-graceful transitions to 'standby', which only occur from interceptors and
		// other signals that the cluster is misconfigured.
		isallowed := !graceful && (role == string(RoleStandby) || role == string(RoleDetectedBrokenConfig))
		if !isallowed {
			return false, fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d with different role, '%s'", role, epoch, c.epoch, c.role)
		}
	}

	changedrole := role != string(c.role)

	if changedrole {
		var err error
		if role == string(RoleStandby) {
			if graceful {
				err = c.gracefulTransitionToStandby(saveConnID)
				if err != nil {
					return false, err
				}
			} else {
				c.immediateTransitionToStandby()
			}
		} else if role == string(RoleDetectedBrokenConfig) {
			c.immediateTransitionToStandby()
		} else {
			c.transitionToPrimary(saveConnID)
		}
	}

	c.role = Role(role)
	c.epoch = epoch

	c.refreshSystemVars()
	c.cinterceptor.setRole(c.role, c.epoch)
	c.sinterceptor.setRole(c.role, c.epoch)
	if changedrole {
		for _, h := range c.commithooks {
			h.setRole(c.role)
		}
	}
	return changedrole, c.persistVariables()
}

func (c *Controller) roleAndEpoch() (Role, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.role, c.epoch
}

func (c *Controller) registerCommitHook(hook *commithook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commithooks = append(c.commithooks, hook)
}

func (c *Controller) GetClusterStatus() []clusterdb.ReplicaStatus {
	if c == nil {
		return []clusterdb.ReplicaStatus{}
	}
	c.mu.Lock()
	epoch, role := c.epoch, c.role
	commithooks := make([]*commithook, len(c.commithooks))
	copy(commithooks, c.commithooks)
	c.mu.Unlock()
	ret := make([]clusterdb.ReplicaStatus, len(commithooks))
	for i, c := range commithooks {
		lag, lastUpdate, currentErrorStr := c.status()
		ret[i] = clusterdb.ReplicaStatus{
			Database:       c.dbname,
			Remote:         c.remotename,
			Role:           string(role),
			Epoch:          epoch,
			ReplicationLag: lag,
			LastUpdate:     lastUpdate,
			CurrentError:   currentErrorStr,
		}
	}
	return ret
}

func (c *Controller) recordSuccessfulRemoteSrvCommit(name string) {
	c.lgr.Tracef("standby replica received push and updated database %s", name)
	c.mu.Lock()
	commithooks := make([]*commithook, len(c.commithooks))
	copy(commithooks, c.commithooks)
	c.mu.Unlock()
	for _, c := range commithooks {
		if c.dbname == name {
			c.recordSuccessfulRemoteSrvCommit()
		}
	}
}

func (c *Controller) RemoteSrvServerArgs(ctx *sql.Context, args remotesrv.ServerArgs) remotesrv.ServerArgs {
	args.HttpPort = c.RemoteSrvPort()
	args.GrpcPort = c.RemoteSrvPort()
	args.Options = c.ServerOptions()
	args = sqle.RemoteSrvServerArgs(ctx, args)
	args.DBCache = remotesrvStoreCache{args.DBCache, c}
	return args
}

// The order of operations is:
// * Set all databases in database_provider to read-only.
// * Kill all running queries in GMS.
// * Replicate all databases to their standby remotes.
//   - If success, return success.
//   - If failure, set all databases in database_provider back to their original state. Return failure.
//
// saveConnID is potentially a connID of the caller to
// dolt_assume_cluster_role(), which should not be killed with the other
// connections. That connection will be transitioned to a terminal error state
// after returning the results of dolt_assume_cluster_role().
//
// called with c.mu held
func (c *Controller) gracefulTransitionToStandby(saveConnID int) error {
	c.setProviderIsStandby(true)
	c.killRunningQueries(saveConnID)
	// TODO: this can block with c.mu held, although we are not too
	// interested in the server proceeding gracefully while this is
	// happening.
	if err := c.waitForHooksToReplicate(); err != nil {
		c.setProviderIsStandby(false)
		c.killRunningQueries(saveConnID)
		return err
	}
	return nil
}

// The order of operations is:
// * Set all databases in database_provider to read-only.
// * Kill all running queries in GMS.
// * Return success. NOTE: we do not attempt to replicate to the standby.
//
// called with c.mu held
func (c *Controller) immediateTransitionToStandby() error {
	c.setProviderIsStandby(true)
	c.killRunningQueries(-1)
	return nil
}

// The order of operations is:
// * Set all databases in database_provider back to their original mode: read-write or read only.
// * Kill all running queries in GMS.
// * Return success.
//
// saveConnID is potentially the connID of the caller to
// dolt_assume_cluster_role().
//
// called with c.mu held
func (c *Controller) transitionToPrimary(saveConnID int) error {
	c.setProviderIsStandby(false)
	c.killRunningQueries(saveConnID)
	return nil
}

// Kills all running queries in the managed GMS engine.
// called with c.mu held
func (c *Controller) killRunningQueries(saveConnID int) {
	if c.iterSessions != nil {
		c.iterSessions(func(session sql.Session) (stop bool, err error) {
			if int(session.ID()) != saveConnID {
				c.killQuery(session.ID())
				c.killConnection(session.ID())
			}
			return
		})
	}
}

// called with c.mu held
func (c *Controller) setProviderIsStandby(standby bool) {
	if c.provider != nil {
		c.provider.SetIsStandby(standby)
	}
}

const waitForHooksToReplicateTimeout = 10 * time.Second

// Called during a graceful transition from primary to standby. Waits until all
// commithooks report nextHead == lastPushedHead.
//
// TODO: make the deadline here configurable or something.
//
// called with c.mu held
func (c *Controller) waitForHooksToReplicate() error {
	caughtup := make([]bool, len(c.commithooks))
	var wg sync.WaitGroup
	wg.Add(len(c.commithooks))
	for li, lch := range c.commithooks {
		i := li
		ch := lch
		if ch.isCaughtUpLocking() {
			caughtup[i] = true
			wg.Done()
		} else {
			ch.setWaitNotify(func() {
				// called with ch.mu locked.
				if !caughtup[i] && ch.isCaughtUp() {
					caughtup[i] = true
					wg.Done()
				}
			})
		}
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	var success bool
	select {
	case <-done:
		success = true
	case <-time.After(waitForHooksToReplicateTimeout):
		success = false
	}
	for _, ch := range c.commithooks {
		ch.setWaitNotify(nil)
	}
	// make certain we don't leak the wg.Wait goroutine in the failure case.
	for _, b := range caughtup {
		if !b {
			wg.Done()
		}
	}
	if success {
		c.lgr.Tracef("cluster/controller: successfully replicated all databases to all standbys; transitioning to standby.")
		return nil
	} else {
		c.lgr.Warnf("cluster/controller: failed to replicate all databases to all standbys; not transitioning to standby.")
		return errors.New("cluster/controller: failed to transition from primary to standby gracefully; could not replicate databases to standby in a timely manner.")
	}
}
