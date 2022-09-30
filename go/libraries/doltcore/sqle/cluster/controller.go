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
	"fmt"
	"strconv"
	"sync"

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

	iterSessions   IterSessions
	killQuery      func(uint32)
	killConnection func(uint32) error
}

type sqlvars interface {
	AddSystemVariables(sysVars []sql.SystemVariable)
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
	ret.sinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.sinterceptor.setRole(role, epoch)
	ret.cinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.cinterceptor.setRole(role, epoch)
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

func (c *Controller) ManageQueryConnections(iterSessions IterSessions, killQuery func(uint32), killConnection func(uint32) error) {
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
		return "", 0, fmt.Errorf("persisted role %s.%s = %s must be \"primary\" or \"secondary\"", PersistentConfigPrefix, DoltClusterRoleVariable, persistentRole)
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

func (c *Controller) setRoleAndEpoch(role string, epoch int, graceful bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch == c.epoch && role == string(c.role) {
		return nil
	}
	if epoch == c.epoch {
		return fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d with different role, '%s'", role, epoch, c.epoch, c.role)
	}
	if epoch < c.epoch {
		return fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d", role, epoch, c.epoch)
	}

	if role != "primary" && role != "standby" {
		return fmt.Errorf("error assuming role '%s'; valid roles are 'primary' and 'standby'", role)
	}

	changedrole := role != string(c.role)

	c.role = Role(role)
	c.epoch = epoch

	if changedrole {
		var err error
		if c.role == RoleStandby {
			if graceful {
				err = c.gracefulTransitionToStandby()
			} else {
				err = c.immediateTransitionToStandby()
				// TODO: this should not fail; if it does, we still prevent transition for now.
			}
		} else {
			err = c.transitionToPrimary()
			// TODO: this should not fail; if it does, we still prevent transition for now.
		}
		if err != nil {
			return err
		}
	}

	c.refreshSystemVars()
	c.cinterceptor.setRole(c.role, c.epoch)
	c.sinterceptor.setRole(c.role, c.epoch)
	for _, h := range c.commithooks {
		h.setRole(c.role)
	}
	return c.persistVariables()
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
func (c *Controller) gracefulTransitionToStandby() error {
	return nil
}

// The order of operations is:
// * Set all databases in database_provider to read-only.
// * Kill all running queries in GMS.
// * Return success. NOTE: we do not attempt to replicate to the standby.
func (c *Controller) immediateTransitionToStandby() error {
	return nil
}

// The order of operations is:
// * Set all databases in database_provider back to their original mode: read-write or read only.
// * Kill all running queries in GMS.
// * Return success.
func (c *Controller) transitionToPrimary() error {
	return nil
}

// Kills all running queries in the managed GMS engine.
func (c *Controller) killRunningQueries() {
	c.mu.Lock()
	iterSessions, killQuery, killConnection := c.iterSessions, c.killQuery, c.killConnection
	c.mu.Unlock()
	if c.iterSessions != nil {
		iterSessions(func(session sql.Session) (stop bool, err error) {
			killQuery(session.ID())
			killConnection(session.ID())
			return
		})
	}
}
