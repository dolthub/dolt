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

package dsess

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

var ErrSessionNotPeristable = errors.New("session is not persistable")

type DoltSession struct {
	*Session
	globalsConf config.ReadWriteConfig
	mu          *sync.Mutex
}

var _ sql.Session = (*DoltSession)(nil)
var _ sql.PersistableSession = (*DoltSession)(nil)

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(ctx *sql.Context, sqlSess *sql.BaseSession, pro RevisionDatabaseProvider, conf config.ReadWriteConfig, dbs ...InitialDbState) (*DoltSession, error) {
	sess, err := NewSession(ctx, sqlSess, pro, conf, dbs...)
	if err != nil {
		return nil, err
	}

	globals := config.NewPrefixConfig(conf, env.SqlServerGlobalsPrefix)
	return sess.NewDoltSession(globals), nil
}

// PersistGlobal implements sql.PersistableSession
func (s *DoltSession) PersistGlobal(sysVarName string, value interface{}) error {
	if s.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return setPersistedValue(s.globalsConf, sysVar.Name, value)
}

// RemovePersistedGlobal implements sql.PersistableSession
func (s *DoltSession) RemovePersistedGlobal(sysVarName string) error {
	if s.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.globalsConf.Unset([]string{sysVar.Name})
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *DoltSession) RemoveAllPersistedGlobals() error {
	if s.globalsConf == nil {
		return ErrSessionNotPeristable
	}

	allVars := make([]string, s.globalsConf.Size())
	i := 0
	s.globalsConf.Iter(func(k, v string) bool {
		allVars[i] = k
		i++
		return false
	})

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.globalsConf.Unset(allVars)
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *DoltSession) GetPersistedValue(k string) (interface{}, error) {
	if s.globalsConf == nil {
		return nil, ErrSessionNotPeristable
	}

	return getPersistedValue(s.globalsConf, k)
}

// SystemVariablesInConfig returns a list of System Variables associated with the session
func (s *DoltSession) SystemVariablesInConfig() ([]sql.SystemVariable, error) {
	if s.globalsConf == nil {
		return nil, ErrSessionNotPeristable
	}

	return SystemVariablesInConfig(s.globalsConf)
}

// validatePersistedSysVar checks whether a system variable exists and is dynamic
func validatePersistableSysVar(name string) (sql.SystemVariable, interface{}, error) {
	sysVar, val, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return sql.SystemVariable{}, nil, sql.ErrUnknownSystemVariable.New(name)
	}
	if !sysVar.Dynamic {
		return sql.SystemVariable{}, nil, sql.ErrSystemVariableReadOnly.New(name)
	}
	return sysVar, val, nil
}

// getPersistedValue reads and converts a config value to the associated SystemVariable type
func getPersistedValue(conf config.ReadableConfig, k string) (interface{}, error) {
	v, err := conf.GetString(k)
	if err != nil {
		return nil, err
	}

	_, value, err := validatePersistableSysVar(k)
	if err != nil {
		return nil, err
	}

	var res interface{}
	switch value.(type) {
	case int8:
		var tmp int64
		tmp, err = strconv.ParseInt(v, 10, 8)
		res = int8(tmp)
	case int, int16, int32, int64:
		res, err = strconv.ParseInt(v, 10, 64)
	case uint, uint8, uint16, uint32, uint64:
		res, err = strconv.ParseUint(v, 10, 64)
	case float32, float64:
		res, err = strconv.ParseFloat(v, 64)
	case bool:
		return nil, sql.ErrInvalidType.New(value)
	case string:
		return v, nil
	default:
		return nil, sql.ErrInvalidType.New(value)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

// setPersistedValue casts and persists a key value pair assuming thread safety
func setPersistedValue(conf config.WritableConfig, key string, value interface{}) error {
	switch v := value.(type) {
	case int:
		return config.SetInt(conf, key, int64(v))
	case int8:
		return config.SetInt(conf, key, int64(v))
	case int16:
		return config.SetInt(conf, key, int64(v))
	case int32:
		return config.SetInt(conf, key, int64(v))
	case int64:
		return config.SetInt(conf, key, v)
	case uint:
		return config.SetUint(conf, key, uint64(v))
	case uint8:
		return config.SetUint(conf, key, uint64(v))
	case uint16:
		return config.SetUint(conf, key, uint64(v))
	case uint32:
		return config.SetUint(conf, key, uint64(v))
	case uint64:
		return config.SetUint(conf, key, v)
	case float32:
		return config.SetFloat(conf, key, float64(v))
	case float64:
		return config.SetFloat(conf, key, v)
	case string:
		return config.SetString(conf, key, v)
	case bool:
		return sql.ErrInvalidType.New(v)
	default:
		return sql.ErrInvalidType.New(v)
	}
}

// SystemVariablesInConfig returns system variables from the persisted config
func SystemVariablesInConfig(conf config.ReadableConfig) ([]sql.SystemVariable, error) {
	allVars := make([]sql.SystemVariable, conf.Size())
	i := 0
	var err error
	var sysVar sql.SystemVariable
	var def interface{}
	conf.Iter(func(k, v string) bool {
		def, err = getPersistedValue(conf, k)
		if err != nil {
			err = fmt.Errorf("key: '%s'; %w", k, err)
			return true
		}
		// getPeristedVal already checked for errors
		sysVar, _, _ = sql.SystemVariables.GetGlobal(k)
		sysVar.Default = def
		allVars[i] = sysVar
		i++
		return false
	})
	if err != nil {
		return nil, err
	}
	return allVars, nil
}

var initMu = sync.Mutex{}

func InitPersistedSystemVars(dEnv *env.DoltEnv) error {
	initMu.Lock()
	defer initMu.Unlock()

	var globals config.ReadWriteConfig
	if localConf, ok := dEnv.Config.GetConfig(env.LocalConfig); ok {
		globals = config.NewPrefixConfig(localConf, env.SqlServerGlobalsPrefix)
	} else if globalConf, ok := dEnv.Config.GetConfig(env.GlobalConfig); ok {
		globals = config.NewPrefixConfig(globalConf, env.SqlServerGlobalsPrefix)
	} else {
		cli.Println("warning: no local or global Dolt configuration found; session is not persistable")
		globals = config.NewMapConfig(make(map[string]string))
	}

	persistedGlobalVars, err := SystemVariablesInConfig(globals)
	if err != nil {
		return err
	}
	sql.SystemVariables.AddSystemVariables(persistedGlobalVars)
	return nil
}
