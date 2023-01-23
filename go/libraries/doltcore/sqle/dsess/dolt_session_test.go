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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/stretchr/testify/assert"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/utils/config"
)

func TestDoltSessionInit(t *testing.T) {
	dsess := DefaultSession(EmptyDatabaseProvider())
	conf := config.NewMapConfig(make(map[string]string))
	assert.Equal(t, conf, dsess.globalsConf)
}

func TestNewPersistedSystemVariables(t *testing.T) {
	dsess := DefaultSession(EmptyDatabaseProvider())
	conf := config.NewMapConfig(map[string]string{"max_connections": "1000"})
	dsess = dsess.WithGlobals(conf)

	sysVars, err := dsess.SystemVariablesInConfig()
	assert.NoError(t, err)

	maxConRes := sysVars[0]
	assert.Equal(t, "max_connections", maxConRes.Name)
	assert.Equal(t, int64(1000), maxConRes.Default)
}

func TestValidatePeristableSystemVar(t *testing.T) {
	tests := []struct {
		Name string
		Err  *errors.Kind
	}{
		{
			Name: "max_connections",
			Err:  nil,
		},
		{
			Name: "init_file",
			Err:  sql.ErrSystemVariableReadOnly,
		},
		{
			Name: "unknown",
			Err:  sql.ErrUnknownSystemVariable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if sysVar, _, err := validatePersistableSysVar(tt.Name); tt.Err != nil {
				assert.True(t, tt.Err.Is(err))
			} else {
				assert.Equal(t, tt.Name, sysVar.Name)

			}
		})
	}
}

func TestSetPersistedValue(t *testing.T) {
	tests := []struct {
		Name        string
		Value       interface{}
		ExpectedRes interface{}
		Err         *errors.Kind
	}{
		{
			Name:  "int",
			Value: 7,
		},
		{
			Name:  "int8",
			Value: int8(7),
		},
		{
			Name:  "int16",
			Value: int16(7),
		},
		{
			Name:  "int32",
			Value: int32(7),
		},
		{
			Name:  "int64",
			Value: int64(7),
		},
		{
			Name:  "uint",
			Value: uint(7),
		},
		{
			Name:  "uint8",
			Value: uint8(7),
		},
		{
			Name:  "uint16",
			Value: uint16(7),
		},
		{
			Name:  "uint32",
			Value: uint32(7),
		},
		{
			Name:  "uint64",
			Value: uint64(7),
		},
		{
			Name:        "float32",
			Value:       float32(7),
			ExpectedRes: "7.00000000",
		},
		{
			Name:        "float64",
			Value:       float64(7),
			ExpectedRes: "7.00000000",
		},
		{
			Name:  "string",
			Value: "7",
		},
		{
			Name:  "bool",
			Value: true,
			Err:   sql.ErrInvalidType,
		},
		{
			Name:  "bool",
			Value: false,
			Err:   sql.ErrInvalidType,
		},
		{
			Value: complex64(7),
			Err:   sql.ErrInvalidType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			conf := config.NewMapConfig(make(map[string]string))
			if err := setPersistedValue(conf, "key", tt.Value); tt.Err != nil {
				assert.True(t, tt.Err.Is(err))
			} else if tt.ExpectedRes == nil {
				assert.Equal(t, "7", conf.GetStringOrDefault("key", ""))
			} else {
				assert.Equal(t, tt.ExpectedRes, conf.GetStringOrDefault("key", ""))

			}
		})
	}
}

func TestGetPersistedValue(t *testing.T) {
	tests := []struct {
		Name        string
		Value       string
		ExpectedRes interface{}
		Err         bool
	}{
		{
			Name:        "long_query_time",
			Value:       "7",
			ExpectedRes: float64(7),
		},
		{
			Name:        "tls_ciphersuites",
			Value:       "7",
			ExpectedRes: "7",
		},
		{
			Name:        "max_connections",
			Value:       "7",
			ExpectedRes: int64(7),
		},
		{
			Name:        "tmp_table_size",
			Value:       "7",
			ExpectedRes: uint64(7),
		},
		{
			Name:  "activate_all_roles_on_login",
			Value: "true",
			Err:   true,
		},
		{
			Name:  "activate_all_roles_on_login",
			Value: "on",
			Err:   true,
		},
		{
			Name:        "activate_all_roles_on_login",
			Value:       "1",
			ExpectedRes: int8(1),
		},
		{
			Name:  "activate_all_roles_on_login",
			Value: "false",
			Err:   true,
		},
		{
			Name:  "activate_all_roles_on_login",
			Value: "off",
			Err:   true,
		},
		{
			Name:        "activate_all_roles_on_login",
			Value:       "0",
			ExpectedRes: int8(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			conf := config.NewMapConfig(map[string]string{tt.Name: tt.Value})
			if val, err := getPersistedValue(conf, tt.Name); tt.Err {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tt.ExpectedRes, val)
			}
		})
	}
}
