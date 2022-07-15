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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const (
	SysVarFalse = int8(0)
	SysVarTrue  = int8(1)
)

func init() {
	AddDoltSystemVariables()
}

func AddDoltSystemVariables() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{
			Name:              dsess.ReplicateToRemoteKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(dsess.ReplicateToRemoteKey),
			Default:           "",
		},
		{
			Name:              dsess.ReadReplicaRemoteKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(dsess.ReadReplicaRemoteKey),
			Default:           "",
		},
		{
			Name:              dsess.SkipReplicationErrorsKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(dsess.SkipReplicationErrorsKey),
			Default:           int8(0),
		},
		{
			Name:              dsess.ReplicateHeadsKey,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(dsess.ReplicateHeadsKey),
			Default:           "",
		},
		{
			Name:              dsess.ReplicateAllHeadsKey,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(dsess.ReplicateAllHeadsKey),
			Default:           int8(0),
		},
		{
			Name:              dsess.AsyncReplicationKey,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(dsess.AsyncReplicationKey),
			Default:           int8(0),
		},
	})
}

func SkipReplicationWarnings() bool {
	_, skip, ok := sql.SystemVariables.GetGlobal(dsess.SkipReplicationErrorsKey)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return skip == SysVarTrue
}
