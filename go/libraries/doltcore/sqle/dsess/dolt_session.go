// Copyright 2020 Dolthub, Inc.
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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/utils/config"
)

type DoltSession struct {
	*Session
	globalsConf config.ReadWriteConfig
}

var _ sql.Session = (*DoltSession)(nil)
var _ sql.PersistableSession = (*DoltSession)(nil)

func NewDoltSession(sess *Session, conf config.ReadWriteConfig) *DoltSession {
	return &DoltSession{Session: sess, globalsConf: conf}
}

// PersistGlobal implements sql.PersistableSession
func (s *DoltSession) PersistGlobal(sysVarName string, value interface{}) error {
	sysVar, _, err := validateSysVar(sysVarName)
	if err != nil {
		return err
	}

	// TODO lock?
	return setConfigValue(s.globalsConf, sysVar.Name, value)
}

// RemovePersistedGlobal implements sql.PersistableSession
func (s *DoltSession) RemovePersistedGlobal(sysVarName string) error {
	sysVar, _, err := validateSysVar(sysVarName)
	if err != nil {
		return err
	}

	// TODO lock?
	return s.globalsConf.Unset([]string{sysVar.Name})
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *DoltSession) RemoveAllPersistedGlobals() error {
	allVars := make([]string, s.globalsConf.Size())
	i := 0
	s.globalsConf.Iter(func(k, v string) bool {
		allVars[i] = k
		i++
		return false
	})

	// TODO lock?
	return s.globalsConf.Unset(allVars)
}

func (s *DoltSession) GetPersistedValue(k string) (interface{}, error) {
	v, err := s.globalsConf.GetString(k)
	if err != nil {
		return nil, err
	}

	sysVar, value, err := validateSysVar(k)
	if err != nil {
		return nil, err
	}

	var res interface{}
	switch value.(type) {
	case int, int8, int16, int32, int64:
		res, err = strconv.ParseInt(v, 10, 64)
	case uint, uint8, uint16, uint32, uint64:
		res, err = strconv.ParseUint(v, 10, 64)
	case float32, float64:
		res, err = strconv.ParseFloat(v, 64)
	case string:
		return v, nil
	default:
		return nil, sql.ErrInvalidType.New(value)
	}

	if err != nil {
		return nil, err
	}

	return sysVar.Type.Convert(res)
}

func validateSysVar(name string) (sql.SystemVariable, interface{}, error) {
	sysVar, val, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return sql.SystemVariable{}, nil, sql.ErrUnknownSystemVariable.New(name)
	}
	if !sysVar.Dynamic {
		return sql.SystemVariable{}, nil, sql.ErrSystemVariableReadOnly.New(name)
	}
	return sysVar, val, nil
}

func setConfigValue(conf config.WritableConfig, key string, value interface{}) error {
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
	default:
		return sql.ErrInvalidType.New(v)
	}
}

func GetPersistedGlobals(conf config.ReadableConfig) ([]sql.SystemVariable, error) {
	allVars := make([]sql.SystemVariable, conf.Size())
	i := 0
	var err error
	var sysVar sql.SystemVariable
	var value interface{}
	conf.Iter(func(k, v string) bool {
		sysVar, value, err = validateSysVar(k)
		if err != nil {
			return true
		}

		switch value.(type) {
		case int, int8, int16, int32, int64:
			sysVar.Default, err = strconv.ParseInt(v, 10, 64)
		case uint, uint8, uint16, uint32, uint64:
			sysVar.Default, err = strconv.ParseUint(v, 10, 64)
		case float32, float64:
			sysVar.Default, err = strconv.ParseFloat(v, 64)
		case string:
			sysVar.Default = v
		default:
			err = sql.ErrInvalidType.New(value)
		}

		if err != nil {
			return true
		}

		allVars[i] = sysVar
		i++
		return false
	})
	if err != nil {
		return nil, err
	}
	return allVars, nil
}
