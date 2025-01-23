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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/clusterdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/jwtauth"
	"github.com/dolthub/dolt/go/store/types"
)

type Role string

const RolePrimary Role = "primary"
const RoleStandby Role = "standby"
const RoleDetectedBrokenConfig Role = "detected_broken_config"

const PersistentConfigPrefix = "sqlserver.cluster"

// State for any ongoing DROP DATABASE replication attempts we have
// outstanding. When we create a database, we cancel all on going DROP DATABASE
// replication attempts.
type databaseDropReplication struct {
	ctx    context.Context
	cancel func()
	wg     *sync.WaitGroup
}

type Controller struct {
	cfg           servercfg.ClusterConfig
	persistentCfg config.ReadWriteConfig
	role          Role
	epoch         int
	systemVars    sqlvars
	mu            sync.Mutex
	commithooks   []*commithook
	sinterceptor  serverinterceptor
	cinterceptor  clientinterceptor
	lgr           *logrus.Logger

	standbyCallback IsStandbyCallback
	iterSessions    IterSessions
	killQuery       func(uint32)
	killConnection  func(uint32) error

	jwks      *jwtauth.MultiJWKS
	tlsCfg    *tls.Config
	grpcCreds credentials.PerRPCCredentials
	pub       ed25519.PublicKey
	priv      ed25519.PrivateKey

	replicationClients []*replicationServiceClient

	mysqlDb          *mysql_db.MySQLDb
	mysqlDbPersister *replicatingMySQLDbPersister
	mysqlDbReplicas  []*mysqlDbReplica

	branchControlController *branch_control.Controller
	branchControlFilesys    filesys.Filesys
	bcReplication           *branchControlReplication

	dropDatabase             func(*sql.Context, string) error
	outstandingDropDatabases map[string]*databaseDropReplication
	remoteSrvDBCache         remotesrv.DBCache
}

type sqlvars interface {
	AddSystemVariables(sysVars []sql.SystemVariable)
	GetGlobal(name string) (sql.SystemVariable, interface{}, bool)
}

// Our IsStandbyCallback gets called with |true| or |false| when the server
// becomes a standby or a primary respectively. Standby replicas should be read
// only.
type IsStandbyCallback func(bool)

type procedurestore interface {
	Register(sql.ExternalStoredProcedureDetails)
}

const (
	// Since we fetch the keys from the other replicas we’re going to use a fixed string here.
	DoltClusterRemoteApiAudience = "dolt-cluster-remote-api.dolthub.com"
)

func NewController(lgr *logrus.Logger, cfg servercfg.ClusterConfig, pCfg config.ReadWriteConfig) (*Controller, error) {
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
		ret.setRoleAndEpoch(role, epoch, roleTransitionOptions{
			graceful: false,
		})
	}
	ret.sinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.sinterceptor.setRole(role, epoch)
	ret.sinterceptor.roleSetter = roleSetter
	ret.cinterceptor.lgr = lgr.WithFields(logrus.Fields{})
	ret.cinterceptor.setRole(role, epoch)
	ret.cinterceptor.roleSetter = roleSetter

	ret.tlsCfg, err = ret.outboundTlsConfig()
	if err != nil {
		return nil, err
	}

	ret.pub, ret.priv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	keyID := creds.PubKeyToKID(ret.pub)
	keyIDStr := creds.B32CredsEncoding.EncodeToString(keyID)
	ret.grpcCreds = &creds.RPCCreds{
		PrivKey:    ret.priv,
		Audience:   DoltClusterRemoteApiAudience,
		Issuer:     creds.ClientIssuer,
		KeyID:      keyIDStr,
		RequireTLS: false,
	}

	ret.jwks = ret.standbyRemotesJWKS()
	ret.sinterceptor.keyProvider = ret.jwks
	ret.sinterceptor.jwtExpected = JWTExpectations()

	ret.replicationClients, err = ret.replicationServiceClients(context.Background())
	if err != nil {
		return nil, err
	}
	ret.mysqlDbReplicas = make([]*mysqlDbReplica, len(ret.replicationClients))
	for i := range ret.mysqlDbReplicas {
		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = time.Second
		bo.MaxInterval = time.Minute
		bo.MaxElapsedTime = 0
		ret.mysqlDbReplicas[i] = &mysqlDbReplica{
			lgr:     lgr.WithFields(logrus.Fields{}),
			client:  ret.replicationClients[i],
			backoff: bo,
		}
		ret.mysqlDbReplicas[i].cond = sync.NewCond(&ret.mysqlDbReplicas[i].mu)
	}

	ret.outstandingDropDatabases = make(map[string]*databaseDropReplication)

	return ret, nil
}

func (c *Controller) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.jwks.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.mysqlDbPersister.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.bcReplication.Run()
	}()
	wg.Wait()
	for _, client := range c.replicationClients {
		client.closer()
	}
}

func (c *Controller) GracefulStop() error {
	c.jwks.GracefulStop()
	c.mysqlDbPersister.GracefulStop()
	c.bcReplication.GracefulStop()
	return nil
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

func (c *Controller) ApplyStandbyReplicationConfig(ctx context.Context, bt *sql.BackgroundThreads, mrEnv *env.MultiRepoEnv, dbs ...dsess.SqlDatabase) error {
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

func (c *Controller) SetIsStandbyCallback(callback IsStandbyCallback) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.standbyCallback = callback
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
		remoteUrl := strings.Replace(r.RemoteURLTemplate(), dsess.URLTemplateDatabasePlaceholder, name, -1)
		remote, ok := remotes.Get(r.Name())
		if !ok {
			remote = env.NewRemote(r.Name(), remoteUrl, nil)
			err := denv.AddRemote(remote)
			if err != nil {
				return nil, fmt.Errorf("sqle: cluster: standby replication: could not create remote %s for database %s: %w", r.Name(), name, err)
			}
		}
		commitHook := newCommitHook(c.lgr, r.Name(), remote.Url, name, c.role, func(ctx context.Context) (*doltdb.DoltDB, error) {
			return remote.GetRemoteDB(ctx, types.Format_Default, dialprovider)
		}, denv.DoltDB(ctx), ttfdir)
		denv.DoltDB(ctx).PrependCommitHook(ctx, commitHook)
		if err := commitHook.Run(bt); err != nil {
			return nil, err
		}
		hooks = append(hooks, commitHook)
	}
	return hooks, nil
}

func (c *Controller) gRPCDialProvider(denv *env.DoltEnv) dbfactory.GRPCDialProvider {
	return grpcDialProvider{env.NewGRPCDialProviderFromDoltEnv(denv), &c.cinterceptor, c.tlsCfg, c.grpcCreds}
}

func (c *Controller) RegisterStoredProcedures(store procedurestore) {
	if c == nil {
		return
	}
	store.Register(newAssumeRoleProcedure(c))
	store.Register(newTransitionToStandbyProcedure(c))
}

// Incoming drop database replication requests need a way to drop a database in
// the sqle.DatabaseProvider. This is our callback for that functionality.
func (c *Controller) SetDropDatabase(dropDatabase func(*sql.Context, string) error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropDatabase = dropDatabase
}

// DropDatabaseHook gets called when the database provider drops a
// database. This is how we learn that we need to replicate a drop database.
func (c *Controller) DropDatabaseHook() func(*sql.Context, string) {
	return c.dropDatabaseHook
}

func (c *Controller) dropDatabaseHook(_ *sql.Context, dbname string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// We always cleanup the commithooks associated with that database.

	j := 0
	for i := 0; i < len(c.commithooks); i++ {
		if c.commithooks[i].dbname == dbname {
			c.commithooks[i].databaseWasDropped()
			continue
		}
		if j != i {
			c.commithooks[j] = c.commithooks[i]
		}
		j += 1
	}
	c.commithooks = c.commithooks[:j]

	if c.role != RolePrimary {
		return
	}

	// If we are the primary, we will replicate the drop to our standby replicas.

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(len(c.replicationClients))
	state := &databaseDropReplication{
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
	}
	c.outstandingDropDatabases[dbname] = state

	for _, client := range c.replicationClients {
		client := client
		go c.replicateDropDatabase(state, client, dbname)
	}
}

func (c *Controller) cancelDropDatabaseReplication(dbname string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.outstandingDropDatabases[dbname]; s != nil {
		s.cancel()
		s.wg.Wait()
	}
}

func (c *Controller) replicateDropDatabase(s *databaseDropReplication, client *replicationServiceClient, dbname string) {
	defer s.wg.Done()
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = time.Millisecond
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = 0
	for {
		if s.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
		_, err := client.client.DropDatabase(ctx, &replicationapi.DropDatabaseRequest{
			Name: dbname,
		})
		cancel()
		if err == nil {
			c.lgr.Tracef("successfully replicated drop of [%s] to %s", dbname, client.remote)
			return
		}
		if status.Code(err) == codes.FailedPrecondition {
			c.lgr.Warnf("drop of [%s] to %s will not be replicated; FailedPrecondition", dbname, client.remote)
			return
		}
		c.lgr.Warnf("failed to replicate drop of [%s] to %s: %v", dbname, client.remote, err)
		if s.ctx.Err() != nil {
			return
		}
		d := bo.NextBackOff()
		c.lgr.Tracef("sleeping %v before next drop attempt for database [%s] at %s", d, dbname, client.remote)
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
			return
		}
	}
}

func (c *Controller) ClusterDatabase() sql.Database {
	if c == nil {
		return nil
	}
	return clusterdb.NewClusterDatabase(c)
}

func (c *Controller) RemoteSrvListenAddr() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.cfg.RemotesAPIConfig().Address(), c.cfg.RemotesAPIConfig().Port())
}

func (c *Controller) ServerOptions() []grpc.ServerOption {
	return c.sinterceptor.Options()
}

func (c *Controller) refreshSystemVars() {
	role, epoch := string(c.role), c.epoch
	vars := []sql.SystemVariable{
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltClusterRoleVariable,
			Dynamic: false,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Persist),
			Type:    gmstypes.NewSystemStringType(dsess.DoltClusterRoleVariable),
			Default: role,
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltClusterRoleEpochVariable,
			Dynamic: false,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Persist),
			Type:    gmstypes.NewSystemIntType(dsess.DoltClusterRoleEpochVariable, 0, 9223372036854775807, false),
			Default: epoch,
		},
	}
	c.systemVars.AddSystemVariables(vars)
}

func (c *Controller) persistVariables() error {
	toset := make(map[string]string)
	toset[dsess.DoltClusterRoleVariable] = string(c.role)
	toset[dsess.DoltClusterRoleEpochVariable] = strconv.Itoa(c.epoch)
	return c.persistentCfg.SetStrings(toset)
}

func applyBootstrapClusterConfig(lgr *logrus.Logger, cfg servercfg.ClusterConfig, pCfg config.ReadWriteConfig) (Role, int, error) {
	toset := make(map[string]string)
	persistentRole := pCfg.GetStringOrDefault(dsess.DoltClusterRoleVariable, "")
	var roleFromPersistentConfig bool
	persistentEpoch := pCfg.GetStringOrDefault(dsess.DoltClusterRoleEpochVariable, "")
	if persistentRole == "" {
		if cfg.BootstrapRole() != "" {
			lgr.Tracef("cluster/controller: persisted cluster role was empty, apply bootstrap_role %s", cfg.BootstrapRole())
			persistentRole = cfg.BootstrapRole()
		} else {
			lgr.Trace("cluster/controller: persisted cluster role was empty, bootstrap_role was empty: defaulted to primary")
			persistentRole = "primary"
		}
		toset[dsess.DoltClusterRoleVariable] = persistentRole
	} else {
		roleFromPersistentConfig = true
		lgr.Tracef("cluster/controller: persisted cluster role is %s", persistentRole)
	}
	if persistentEpoch == "" {
		persistentEpoch = strconv.Itoa(cfg.BootstrapEpoch())
		lgr.Tracef("cluster/controller: persisted cluster role epoch is empty, took boostrap_epoch: %s", persistentEpoch)
		toset[dsess.DoltClusterRoleEpochVariable] = persistentEpoch
	} else {
		lgr.Tracef("cluster/controller: persisted cluster role epoch is %s", persistentEpoch)
	}
	if persistentRole != string(RolePrimary) && persistentRole != string(RoleStandby) {
		isallowed := persistentRole == string(RoleDetectedBrokenConfig) && roleFromPersistentConfig
		if !isallowed {
			return "", 0, fmt.Errorf("persisted role %s.%s = %s must be \"primary\" or \"secondary\"", PersistentConfigPrefix, dsess.DoltClusterRoleVariable, persistentRole)
		}
	}
	epochi, err := strconv.Atoi(persistentEpoch)
	if err != nil {
		return "", 0, fmt.Errorf("persisted role epoch %s.%s = %s must be an integer", PersistentConfigPrefix, dsess.DoltClusterRoleEpochVariable, persistentEpoch)
	}
	if len(toset) > 0 {
		err := pCfg.SetStrings(toset)
		if err != nil {
			return "", 0, err
		}
	}
	return Role(persistentRole), epochi, nil
}

type roleTransitionOptions struct {
	// If true, all standby replicas must be caught up in order to
	// transition from primary to standby.
	graceful bool

	// If non-zero and |graceful| is true, will allow a transition from
	// primary to standby to succeed only if this many standby replicas
	// are known to be caught up at the finalization of the replication
	// hooks.
	minCaughtUpStandbys int

	// If non-nil, this connection will be saved if and when the connection
	// process needs to terminate existing connections.
	saveConnID *int
}

type roleTransitionResult struct {
	// true if the role changed as a result of this call.
	changedRole bool

	// filled in with graceful transition results if this was a graceful
	// transition and it was successful.
	gracefulTransitionResults []graceTransitionResult
}

func (c *Controller) setRoleAndEpoch(role string, epoch int, opts roleTransitionOptions) (roleTransitionResult, error) {
	graceful := opts.graceful
	saveConnID := -1
	if opts.saveConnID != nil {
		saveConnID = *opts.saveConnID
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch == c.epoch && role == string(c.role) {
		return roleTransitionResult{false, nil}, nil
	}

	if role != string(RolePrimary) && role != string(RoleStandby) && role != string(RoleDetectedBrokenConfig) {
		return roleTransitionResult{false, nil}, fmt.Errorf("error assuming role '%s'; valid roles are 'primary' and 'standby'", role)
	}

	if epoch < c.epoch {
		return roleTransitionResult{false, nil}, fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d", role, epoch, c.epoch)
	}
	if epoch == c.epoch {
		// This is allowed for non-graceful transitions to 'standby', which only occur from interceptors and
		// other signals that the cluster is misconfigured.
		isallowed := !graceful && (role == string(RoleStandby) || role == string(RoleDetectedBrokenConfig))
		if !isallowed {
			return roleTransitionResult{false, nil}, fmt.Errorf("error assuming role '%s' at epoch %d; already at epoch %d with different role, '%s'", role, epoch, c.epoch, c.role)
		}
	}

	changedrole := role != string(c.role)
	var gracefulResults []graceTransitionResult

	if changedrole {
		var err error
		if role == string(RoleStandby) {
			if graceful {
				beforeRole, beforeEpoch := c.role, c.epoch
				gracefulResults, err = c.gracefulTransitionToStandby(saveConnID, opts.minCaughtUpStandbys)
				if err == nil && (beforeRole != c.role || beforeEpoch != c.epoch) {
					// The role or epoch moved out from under us while we were unlocked and transitioning to standby.
					err = fmt.Errorf("error assuming role '%s' at epoch %d: the role configuration changed while we were replicating to our standbys. Please try again", role, epoch)
				}
				if err != nil {
					c.setProviderIsStandby(c.role != RolePrimary)
					c.killRunningQueries(saveConnID)
					return roleTransitionResult{false, nil}, err
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
		c.mysqlDbPersister.setRole(c.role)
		c.bcReplication.setRole(c.role)
	}
	_ = c.persistVariables()
	return roleTransitionResult{
		changedRole:               changedrole,
		gracefulTransitionResults: gracefulResults,
	}, nil
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

func (c *Controller) RemoteSrvServerArgs(ctxFactory func(context.Context) (*sql.Context, error), args remotesrv.ServerArgs) (remotesrv.ServerArgs, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	listenaddr := c.RemoteSrvListenAddr()
	args.HttpListenAddr = listenaddr
	args.GrpcListenAddr = listenaddr
	args.Options = c.ServerOptions()
	var err error
	args.FS, args.DBCache, err = sqle.RemoteSrvFSAndDBCache(ctxFactory, sqle.CreateUnknownDatabases)
	if err != nil {
		return remotesrv.ServerArgs{}, err
	}
	args.DBCache = remotesrvStoreCache{args.DBCache, c}
	c.remoteSrvDBCache = args.DBCache

	keyID := creds.PubKeyToKID(c.pub)
	keyIDStr := creds.B32CredsEncoding.EncodeToString(keyID)
	args.HttpInterceptor = JWKSHandlerInterceptor(keyIDStr, c.pub)

	return args, nil
}

func (c *Controller) HookMySQLDbPersister(persister MySQLDbPersister, mysqlDb *mysql_db.MySQLDb) MySQLDbPersister {
	if c != nil {
		c.mysqlDb = mysqlDb
		c.mysqlDbPersister = &replicatingMySQLDbPersister{
			base:     persister,
			replicas: c.mysqlDbReplicas,
		}
		c.mysqlDbPersister.setRole(c.role)
		persister = c.mysqlDbPersister
	}
	return persister
}

func (c *Controller) HookBranchControlPersistence(controller *branch_control.Controller, fs filesys.Filesys) {
	if c != nil {
		c.branchControlController = controller
		c.branchControlFilesys = fs

		replicas := make([]*branchControlReplica, len(c.replicationClients))
		for i := range replicas {
			bo := backoff.NewExponentialBackOff()
			bo.InitialInterval = time.Second
			bo.MaxInterval = time.Minute
			bo.MaxElapsedTime = 0
			replicas[i] = &branchControlReplica{
				backoff: bo,
				client:  c.replicationClients[i],
				lgr:     c.lgr.WithFields(logrus.Fields{}),
			}
			replicas[i].cond = sync.NewCond(&replicas[i].mu)
		}
		c.bcReplication = &branchControlReplication{
			replicas:     replicas,
			bcController: controller,
		}
		c.bcReplication.setRole(c.role)

		controller.SavedCallback = func(ctx context.Context) {
			contents := controller.Serialized.Load()
			if contents != nil {
				var rsc doltdb.ReplicationStatusController
				c.bcReplication.UpdateBranchControlContents(ctx, *contents, &rsc)
				if sqlCtx, ok := ctx.(*sql.Context); ok {
					dsess.WaitForReplicationController(sqlCtx, rsc)
				}
			}
		}
	}
}

func (c *Controller) RegisterGrpcServices(ctxFactory func(context.Context) (*sql.Context, error), srv *grpc.Server) {
	replicationapi.RegisterReplicationServiceServer(srv, &replicationServiceServer{
		ctxFactory:           ctxFactory,
		mysqlDb:              c.mysqlDb,
		branchControl:        c.branchControlController,
		branchControlFilesys: c.branchControlFilesys,
		dropDatabase:         c.dropDatabase,
		lgr:                  c.lgr.WithFields(logrus.Fields{}),
	})
}

// TODO: make the deadline here configurable or something.
const waitForHooksToReplicateTimeout = 10 * time.Second

type graceTransitionResult struct {
	caughtUp  bool
	database  string
	remote    string
	remoteUrl string
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
func (c *Controller) gracefulTransitionToStandby(saveConnID, minCaughtUpStandbys int) ([]graceTransitionResult, error) {
	c.setProviderIsStandby(true)
	c.killRunningQueries(saveConnID)

	var hookStates, mysqlStates, bcStates []graceTransitionResult
	var hookErr, mysqlErr, bcErr error

	// We concurrently wait for hooks, mysql and dolt_branch_control replication to true up.
	// If we encounter any errors while doing this, we fail the graceful transition.

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		// waitForHooksToReplicate will release the lock while it
		// blocks, but will return with the lock held.
		hookStates, hookErr = c.waitForHooksToReplicate(waitForHooksToReplicateTimeout)
	}()
	go func() {
		defer wg.Done()
		mysqlStates, mysqlErr = c.mysqlDbPersister.waitForReplication(waitForHooksToReplicateTimeout)
	}()
	go func() {
		defer wg.Done()
		bcStates, bcErr = c.bcReplication.waitForReplication(waitForHooksToReplicateTimeout)
	}()
	wg.Wait()

	if hookErr != nil {
		return nil, hookErr
	}
	if mysqlErr != nil {
		return nil, mysqlErr
	}
	if bcErr != nil {
		return nil, bcErr
	}

	if len(hookStates) != len(c.commithooks) {
		c.lgr.Warnf("cluster/controller: failed to transition to standby; the set of replicated databases changed during the transition.")
		return nil, errors.New("cluster/controller: failed to transition to standby; the set of replicated databases changed during the transition.")
	}

	res := make([]graceTransitionResult, 0, len(hookStates)+len(mysqlStates)+len(bcStates))
	res = append(res, hookStates...)
	res = append(res, mysqlStates...)
	res = append(res, bcStates...)

	if minCaughtUpStandbys == 0 {
		for _, state := range res {
			if !state.caughtUp {
				c.lgr.Warnf("cluster/controller: failed to replicate all databases to all standbys; not transitioning to standby.")
				return nil, fmt.Errorf("cluster/controller: failed to transition from primary to standby gracefully; could not replicate databases to standby in a timely manner.")
			}
		}
		c.lgr.Tracef("cluster/controller: successfully replicated all databases to all standbys; transitioning to standby.")
	} else {
		databases := make(map[string]struct{})
		replicas := make(map[string]int)
		for _, r := range res {
			databases[r.database] = struct{}{}
			url, err := url.Parse(r.remoteUrl)
			if err != nil {
				return nil, fmt.Errorf("cluster/controller: could not parse remote_url (%s) for remote %s on database %s: %w", r.remoteUrl, r.remote, r.database, err)
			}
			if _, ok := replicas[url.Host]; !ok {
				replicas[url.Host] = 0
			}
			if r.caughtUp {
				replicas[url.Host] = replicas[url.Host] + 1
			}
		}
		numCaughtUp := 0
		for _, v := range replicas {
			if v == len(databases) {
				numCaughtUp += 1
			}
		}
		if numCaughtUp < minCaughtUpStandbys {
			return nil, fmt.Errorf("cluster/controller: failed to transition from primary to standby gracefully; could not ensure %d replicas were caught up on all %d databases. Only caught up %d standbys fully.", minCaughtUpStandbys, len(databases), numCaughtUp)
		}
		c.lgr.Tracef("cluster/controller: successfully replicated all databases to %d out of %d standbys; transitioning to standby.", numCaughtUp, len(replicas))
	}

	return res, nil
}

func allCaughtUp(res []graceTransitionResult) bool {
	for _, r := range res {
		if !r.caughtUp {
			return false
		}
	}
	return true
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
	if c.standbyCallback != nil {
		c.standbyCallback(standby)
	}
}

// Called during a graceful transition from primary to standby. Waits until all
// commithooks report nextHead == lastPushedHead.
//
// Returns `[]bool` with an entry for each `commithook` which existed at the
// start of the call. The entry will be `true` if that `commithook` was caught
// up as part of this wait, and `false` otherwise.
//
// called with c.mu held
func (c *Controller) waitForHooksToReplicate(timeout time.Duration) ([]graceTransitionResult, error) {
	commithooks := make([]*commithook, len(c.commithooks))
	copy(commithooks, c.commithooks)
	res := make([]graceTransitionResult, len(commithooks))
	for i := range res {
		res[i].database = commithooks[i].dbname
		res[i].remote = commithooks[i].remotename
		res[i].remoteUrl = commithooks[i].remoteurl
	}
	var wg sync.WaitGroup
	wg.Add(len(commithooks))
	for li, lch := range commithooks {
		i := li
		ch := lch
		ok := ch.setWaitNotify(func() {
			// called with ch.mu locked.
			if !res[i].caughtUp && ch.isCaughtUp() {
				res[i].caughtUp = true
				wg.Done()
			}
		})
		if !ok {
			for j := li - 1; j >= 0; j-- {
				commithooks[j].setWaitNotify(nil)
			}
			c.lgr.Warnf("cluster/controller: failed to wait for graceful transition to standby; there were concurrent attempts to transition..")
			return nil, errors.New("cluster/controller: failed to transition from primary to standby gracefully; did not gain exclusive access to commithooks.")
		}
	}
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	c.mu.Lock()
	for _, ch := range commithooks {
		ch.setWaitNotify(nil)
	}

	// Make certain we don't leak the wg.Wait goroutine in the failure case.
	// At this point, none of the callbacks will ever be called again and
	// ch.setWaitNotify grabs a lock and so establishes the happens before.
	for _, b := range res {
		if !b.caughtUp {
			wg.Done()
		}
	}
	<-done

	return res, nil
}

// Within a cluster, if remotesapi is configured with a tls_ca, we take the
// following semantics:
// * The configured tls_ca file holds a set of PEM encoded x509 certificates,
// all of which are trusted roots for the outbound connections the
// remotestorage client establishes.
// * The certificate chain presented by the server must validate to a root
// which was present in tls_ca. In particular, every certificate in the chain
// must be within its validity window, the signatures must be valid, key usage
// and isCa must be correctly set for the roots and the intermediates, and the
// leaf must have extended key usage server auth.
// * On the other hand, no verification is done against the SAN or the Subject
// of the certificate.
//
// We use these TLS semantics for both connections to the gRPC endpoint which
// is the actual remotesapi, and for connections to any HTTPS endpoints to
// which the gRPC service returns URLs. For now, this works perfectly for our
// use case, but it's tightly coupled to `cluster:` deployment topologies and
// the likes.
//
// If tls_ca is not set then default TLS handling is performed. In particular,
// if the remotesapi endpoints is HTTPS, then the system roots are used and
// ServerName is verified against the presented URL SANs of the certificates.
//
// This tls Config is used for fetching JWKS, for outbound GRPC connections and
// for outbound https connections on the URLs that the GRPC services return.
func (c *Controller) outboundTlsConfig() (*tls.Config, error) {
	tlsCA := c.cfg.RemotesAPIConfig().TLSCA()
	if tlsCA == "" {
		return nil, nil
	}
	urlmatches := c.cfg.RemotesAPIConfig().ServerNameURLMatches()
	dnsmatches := c.cfg.RemotesAPIConfig().ServerNameDNSMatches()
	pem, err := os.ReadFile(tlsCA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("error loading ca roots from " + tlsCA)
	}
	verifyFunc := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		certs := make([]*x509.Certificate, len(rawCerts))
		var err error
		for i, asn1Data := range rawCerts {
			certs[i], err = x509.ParseCertificate(asn1Data)
			if err != nil {
				return err
			}
		}
		keyUsages := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		opts := x509.VerifyOptions{
			Roots:         roots,
			CurrentTime:   time.Now(),
			Intermediates: x509.NewCertPool(),
			KeyUsages:     keyUsages,
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err = certs[0].Verify(opts)
		if err != nil {
			return err
		}
		if len(urlmatches) > 0 {
			found := false
			for _, n := range urlmatches {
				for _, cn := range certs[0].URIs {
					if n == cn.String() {
						found = true
					}
					break
				}
				if found {
					break
				}
			}
			if !found {
				return errors.New("expected certificate to match something in server_name_urls, but it did not")
			}
		}
		if len(dnsmatches) > 0 {
			found := false
			for _, n := range dnsmatches {
				for _, cn := range certs[0].DNSNames {
					if n == cn {
						found = true
					}
					break
				}
				if found {
					break
				}
			}
			if !found {
				return errors.New("expected certificate to match something in server_name_dns, but it did not")
			}
		}
		return nil
	}
	return &tls.Config{
		// We have to InsecureSkipVerify because ServerName is always
		// set by the grpc dial provider and golang tls.Config does not
		// have good support for performing certificate validation
		// without server name validation.
		InsecureSkipVerify: true,

		VerifyPeerCertificate: verifyFunc,

		NextProtos: []string{"h2"},
	}, nil
}

func (c *Controller) standbyRemotesJWKS() *jwtauth.MultiJWKS {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   c.tlsCfg,
			ForceAttemptHTTP2: true,
		},
	}
	urls := make([]string, len(c.cfg.StandbyRemotes()))
	for i, r := range c.cfg.StandbyRemotes() {
		urls[i] = strings.Replace(r.RemoteURLTemplate(), dsess.URLTemplateDatabasePlaceholder, ".well-known/jwks.json", -1)
	}
	return jwtauth.NewMultiJWKS(c.lgr.WithFields(logrus.Fields{"component": "jwks-key-provider"}), urls, client)
}

type replicationServiceClient struct {
	remote string
	url    string
	tls    bool
	client replicationapi.ReplicationServiceClient
	closer func() error
}

func (c *Controller) replicationServiceDialOptions() []grpc.DialOption {
	var ret []grpc.DialOption
	if c.tlsCfg == nil {
		ret = append(ret, grpc.WithInsecure())
	} else {
		ret = append(ret, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsCfg)))
	}

	ret = append(ret, grpc.WithStreamInterceptor(c.cinterceptor.Stream()))
	ret = append(ret, grpc.WithUnaryInterceptor(c.cinterceptor.Unary()))

	ret = append(ret, grpc.WithPerRPCCredentials(c.grpcCreds))

	return ret
}

func (c *Controller) replicationServiceClients(ctx context.Context) ([]*replicationServiceClient, error) {
	var ret []*replicationServiceClient
	for _, r := range c.cfg.StandbyRemotes() {
		urlStr := strings.Replace(r.RemoteURLTemplate(), dsess.URLTemplateDatabasePlaceholder, "", -1)
		url, err := url.Parse(urlStr)
		if err != nil {
			return nil, fmt.Errorf("could not parse remote url template [%s] for remote %s: %w", r.RemoteURLTemplate(), r.Name(), err)
		}
		grpcTarget := "dns:" + url.Hostname() + ":" + url.Port()
		cc, err := grpc.DialContext(ctx, grpcTarget, c.replicationServiceDialOptions()...)
		if err != nil {
			return nil, fmt.Errorf("could not dial grpc endpoint [%s] for remote %s: %w", grpcTarget, r.Name(), err)
		}
		client := replicationapi.NewReplicationServiceClient(cc)
		ret = append(ret, &replicationServiceClient{
			remote: r.Name(),
			url:    grpcTarget,
			tls:    c.tlsCfg != nil,
			client: client,
			closer: cc.Close,
		})
	}
	return ret, nil
}

// Generally r.url is a gRPC dial endpoint and will be something like "dns:53.78.2.1:3832", or something like that.
//
// We want to match these endpoints up with Dolt remotes URLs, which will typically be something like http://53.78.2.1:3832.
func (r *replicationServiceClient) httpUrl() string {
	prefix := "https://"
	if !r.tls {
		prefix = "http://"
	}
	return prefix + strings.TrimPrefix(r.url, "dns:")
}
