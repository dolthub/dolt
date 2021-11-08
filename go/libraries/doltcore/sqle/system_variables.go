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
	DefaultBranchKey         = "dolt_default_branch"
	ReplicateToRemoteKey     = "dolt_replicate_to_remote"
	ReadReplicaRemoteKey     = "dolt_read_replica_remote"
	SkipReplicationErrorsKey = "dolt_skip_replication_errors"
	CurrentBatchModeKey      = "batch_mode"
)

func AddDoltSystemVariables() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{
			Name:              CurrentBatchModeKey,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemIntType(CurrentBatchModeKey, -9223372036854775808, 9223372036854775807, false),
			Default:           int64(0),
		},
		{
			Name:              DefaultBranchKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(DefaultBranchKey),
			Default:           "",
		},
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
	})
}
