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
)

const (
	ReplicateToRemoteKey     = "dolt_replicate_to_remote"
	ReadReplicaRemoteKey     = "dolt_read_replica_remote"
	SkipReplicationErrorsKey = "dolt_skip_replication_errors"
	ReplicateHeadsKey        = "dolt_replicate_heads"
	ReplicateAllHeadsKey     = "dolt_replicate_all_heads"
	AsyncReplicationKey      = "dolt_async_replication"
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
			Name:              ReplicateToRemoteKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(ReplicateToRemoteKey),
			Default:           "",
		},
		{
			Name:              ReadReplicaRemoteKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(ReadReplicaRemoteKey),
			Default:           "",
		},
		{
			Name:              SkipReplicationErrorsKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(SkipReplicationErrorsKey),
			Default:           int8(0),
		},
		{
			Name:              ReplicateHeadsKey,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(ReplicateHeadsKey),
			Default:           "",
		},
		{
			Name:              ReplicateAllHeadsKey,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(ReplicateAllHeadsKey),
			Default:           int8(0),
		},
		{
			Name:              AsyncReplicationKey,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(AsyncReplicationKey),
			Default:           int8(0),
		},
	})
}

func SkipReplicationWarnings() bool {
	_, skip, ok := sql.SystemVariables.GetGlobal(SkipReplicationErrorsKey)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return skip == SysVarTrue
}
