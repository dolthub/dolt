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
}

type sqlvars interface {
	AddSystemVariables(sysVars []sql.SystemVariable)
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

func (c *Controller) ClusterRole() (Role, int) {
	return c.role, c.epoch
}

func (c *Controller) ManageSystemVariables(variables sqlvars) {
	c.systemVars = variables
	c.refreshSystemVars()
}

func (c *Controller) refreshSystemVars() {
	role, epoch := c.ClusterRole()
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
