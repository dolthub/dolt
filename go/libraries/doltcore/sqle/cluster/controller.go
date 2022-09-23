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
	"fmt"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/utils/config"
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
	sinterceptor  serverinterceptor
	cinterceptor  clientinterceptor
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

func NewController(cfg Config, pCfg config.ReadWriteConfig) (*Controller, error) {
	if cfg == nil {
		return nil, nil
	}
	pCfg = config.NewPrefixConfig(pCfg, PersistentConfigPrefix)
	role, epoch, err := applyBootstrapClusterConfig(cfg, pCfg)
	if err != nil {
		return nil, err
	}
	return &Controller{
		cfg:           cfg,
		persistentCfg: pCfg,
		role:          role,
		epoch:         epoch,
	}, nil
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

func (c *Controller) RegisterStoredProcedures(store procedurestore) {
	if c == nil {
		return
	}
	store.Register(newAssumeRoleProcedure(c))
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

func (c *Controller) DialOptions() []grpc.DialOption {
	return c.cinterceptor.Options()
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

func applyBootstrapClusterConfig(cfg Config, pCfg config.ReadWriteConfig) (Role, int, error) {
	toset := make(map[string]string)
	persistentRole := pCfg.GetStringOrDefault(DoltClusterRoleVariable, "")
	persistentEpoch := pCfg.GetStringOrDefault(DoltClusterRoleEpochVariable, "")
	if persistentRole == "" {
		if cfg.BootstrapRole() != "" {
			persistentRole = cfg.BootstrapRole()
		} else {
			persistentRole = "primary"
		}
		toset[DoltClusterRoleVariable] = persistentRole
	}
	if persistentEpoch == "" {
		persistentEpoch = strconv.Itoa(cfg.BootstrapEpoch())
		toset[DoltClusterRoleEpochVariable] = persistentEpoch
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

func (c *Controller) setRoleAndEpoch(role string, epoch int) error {
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
		// TODO: Role is transitioning...lots of stuff to do.
	}

	c.refreshSystemVars()
	c.cinterceptor.setRole(c.role, c.epoch)
	c.sinterceptor.setRole(c.role, c.epoch)
	return c.persistVariables()
}
