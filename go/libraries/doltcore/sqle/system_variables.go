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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// TODO: get rid of me, use an integration point to define new sysvars
func init() {
	AddDoltSystemVariables()
}

func AddDoltSystemVariables() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{
			Name:              dsess.DoltOverrideSchema,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.DoltOverrideSchema),
			Default:           "",
		},
		{
			Name:              dsess.ReplicateToRemote,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicateToRemote),
			Default:           "",
		},
		{
			Name:              dsess.ReplicationRemoteURLTemplate,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicationRemoteURLTemplate),
			Default:           "",
		},
		{
			Name:              dsess.ReadReplicaRemote,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReadReplicaRemote),
			Default:           "",
		},
		{
			Name:              dsess.ReadReplicaForcePull,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ReadReplicaForcePull),
			Default:           int8(1),
		},
		{
			Name:              dsess.SkipReplicationErrors,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.SkipReplicationErrors),
			Default:           int8(0),
		},
		{
			Name:              dsess.ReplicateHeads,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicateHeads),
			Default:           "",
		},
		{
			Name:              dsess.ReplicateAllHeads,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ReplicateAllHeads),
			Default:           int8(0),
		},
		{
			Name:              dsess.AsyncReplication,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.AsyncReplication),
			Default:           int8(0),
		},
		{ // If true, causes a Dolt commit to occur when you commit a transaction.
			Name:              dsess.DoltCommitOnTransactionCommit,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.DoltCommitOnTransactionCommit),
			Default:           int8(0),
		},
		{ // If set, use this message for automatic Dolt commits
			Name:              dsess.DoltCommitOnTransactionCommitMessage,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.DoltCommitOnTransactionCommitMessage),
			Default:           nil,
		},
		{
			Name:              dsess.TransactionsDisabledSysVar,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.TransactionsDisabledSysVar),
			Default:           int8(0),
		},
		{ // If true, disables the conflict and constraint violation check when you commit a transaction.
			Name:              dsess.ForceTransactionCommit,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ForceTransactionCommit),
			Default:           int8(0),
		},
		{
			Name:              dsess.CurrentBatchModeKey,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemIntType(dsess.CurrentBatchModeKey, -9223372036854775808, 9223372036854775807, false),
			Default:           int64(0),
		},
		{ // If true, disables the conflict violation check when you commit a transaction.
			Name:              dsess.AllowCommitConflicts,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.AllowCommitConflicts),
			Default:           int8(0),
		},
		{
			Name:              dsess.AwsCredsFile,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsFile),
			Default:           nil,
		},
		{
			Name:              dsess.AwsCredsProfile,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsProfile),
			Default:           nil,
		},
		{
			Name:              dsess.AwsCredsRegion,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsRegion),
			Default:           nil,
		},
		{
			Name:              dsess.ShowBranchDatabases,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ShowBranchDatabases),
			Default:           int8(0),
		},
		{
			Name:    dsess.DoltClusterAckWritesTimeoutSecs,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Persist,
			Type:    types.NewSystemIntType(dsess.DoltClusterAckWritesTimeoutSecs, 0, 60, false),
			Default: int64(0),
		},
		{
			Name:    dsess.ShowSystemTables,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Both,
			Type:    types.NewSystemBoolType(dsess.ShowSystemTables),
			Default: int8(0),
		},
		{
			Name:    "dolt_dont_merge_json",
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Both,
			Type:    types.NewSystemBoolType("dolt_dont_merge_json"),
			Default: int8(0),
		},
		{
			Name:    dsess.DoltStatsAutoRefreshEnabled,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Global,
			Type:    types.NewSystemBoolType(dsess.DoltStatsAutoRefreshEnabled),
			Default: int8(0),
		},
		{
			Name:    dsess.DoltStatsMemoryOnly,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Global,
			Type:    types.NewSystemBoolType(dsess.DoltStatsMemoryOnly),
			Default: int8(0),
		},
		{
			Name:    dsess.DoltStatsAutoRefreshThreshold,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Global,
			Type:    types.NewSystemDoubleType(dsess.DoltStatsAutoRefreshEnabled, 0, 10),
			Default: float64(.5),
		},
		{
			Name:    dsess.DoltStatsAutoRefreshInterval,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Global,
			Type:    types.NewSystemIntType(dsess.DoltStatsAutoRefreshInterval, 0, math.MaxInt, false),
			Default: 12000,
		},
		{
			Name:    dsess.DoltStatsBranches,
			Dynamic: true,
			Scope:   sql.SystemVariableScope_Global,
			Type:    types.NewSystemStringType(dsess.DoltStatsBranches),
			Default: "",
		},
	})
}

func ReadReplicaForcePull() bool {
	_, forcePull, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaForcePull)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return forcePull == dsess.SysVarTrue
}
