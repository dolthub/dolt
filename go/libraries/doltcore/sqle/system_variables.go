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

var DoltSystemVariables = []sql.SystemVariable{
	&sql.MysqlSystemVariable{
		Name:              "log_bin_branch",
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Persist),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType("log_bin_branch"),
		Default:           "main",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.DoltOverrideSchema,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.DoltOverrideSchema),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReplicateToRemote,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.ReplicateToRemote),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReplicationRemoteURLTemplate,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.ReplicationRemoteURLTemplate),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReadReplicaRemote,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.ReadReplicaRemote),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReadReplicaForcePull,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.ReadReplicaForcePull),
		Default:           int8(1),
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.SkipReplicationErrors,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.SkipReplicationErrors),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReplicateHeads,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.ReplicateHeads),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ReplicateAllHeads,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.ReplicateAllHeads),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.AsyncReplication,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.AsyncReplication),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{ // If true, causes a Dolt commit to occur when you commit a transaction.
		Name:              dsess.DoltCommitOnTransactionCommit,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.DoltCommitOnTransactionCommit),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{ // If set, use this message for automatic Dolt commits
		Name:              dsess.DoltCommitOnTransactionCommitMessage,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.DoltCommitOnTransactionCommitMessage),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.TransactionsDisabledSysVar,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.TransactionsDisabledSysVar),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{ // If true, disables the conflict and constraint violation check when you commit a transaction.
		Name:              dsess.ForceTransactionCommit,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.ForceTransactionCommit),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.CurrentBatchModeKey,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemIntType(dsess.CurrentBatchModeKey, -9223372036854775808, 9223372036854775807, false),
		Default:           int64(0),
	},
	&sql.MysqlSystemVariable{ // If true, disables the conflict violation check when you commit a transaction.
		Name:              dsess.AllowCommitConflicts,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.AllowCommitConflicts),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.AwsCredsFile,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.AwsCredsFile),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.AwsCredsProfile,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.AwsCredsProfile),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.AwsCredsRegion,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              types.NewSystemStringType(dsess.AwsCredsRegion),
		Default:           "",
	},
	&sql.MysqlSystemVariable{
		Name:              dsess.ShowBranchDatabases,
		Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              types.NewSystemBoolType(dsess.ShowBranchDatabases),
		Default:           int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltClusterAckWritesTimeoutSecs,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Persist),
		Type:    types.NewSystemIntType(dsess.DoltClusterAckWritesTimeoutSecs, 0, 60, false),
		Default: int64(0),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.ShowSystemTables,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Type:    types.NewSystemBoolType(dsess.ShowSystemTables),
		Default: int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    "dolt_dont_merge_json",
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Type:    types.NewSystemBoolType("dolt_dont_merge_json"),
		Default: int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    "dolt_optimize_json",
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
		Type:    types.NewSystemBoolType("dolt_optimize_json"),
		Default: int8(1),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsAutoRefreshEnabled,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemBoolType(dsess.DoltStatsAutoRefreshEnabled),
		Default: int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsBootstrapEnabled,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemBoolType(dsess.DoltStatsBootstrapEnabled),
		Default: int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsMemoryOnly,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemBoolType(dsess.DoltStatsMemoryOnly),
		Default: int8(0),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsAutoRefreshThreshold,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemDoubleType(dsess.DoltStatsAutoRefreshThreshold, 0, 10),
		Default: float64(.5),
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsAutoRefreshInterval,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemIntType(dsess.DoltStatsAutoRefreshInterval, 0, math.MaxInt, false),
		Default: 600,
	},
	&sql.MysqlSystemVariable{
		Name:    dsess.DoltStatsBranches,
		Dynamic: true,
		Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
		Type:    types.NewSystemStringType(dsess.DoltStatsBranches),
		Default: "",
	},
}

func AddDoltSystemVariables() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		&sql.MysqlSystemVariable{
			Name:              "log_bin_branch",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Persist),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType("log_bin_branch"),
			Default:           "main",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.DoltOverrideSchema,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.DoltOverrideSchema),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReplicateToRemote,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicateToRemote),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReplicationRemoteURLTemplate,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicationRemoteURLTemplate),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReadReplicaRemote,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReadReplicaRemote),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReadReplicaForcePull,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ReadReplicaForcePull),
			Default:           int8(1),
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.SkipReplicationErrors,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.SkipReplicationErrors),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReplicateHeads,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.ReplicateHeads),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ReplicateAllHeads,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ReplicateAllHeads),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.AsyncReplication,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.AsyncReplication),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{ // If true, causes a Dolt commit to occur when you commit a transaction.
			Name:              dsess.DoltCommitOnTransactionCommit,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.DoltCommitOnTransactionCommit),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{ // If set, use this message for automatic Dolt commits
			Name:              dsess.DoltCommitOnTransactionCommitMessage,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.DoltCommitOnTransactionCommitMessage),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.TransactionsDisabledSysVar,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.TransactionsDisabledSysVar),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{ // If true, disables the conflict and constraint violation check when you commit a transaction.
			Name:              dsess.ForceTransactionCommit,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ForceTransactionCommit),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.CurrentBatchModeKey,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemIntType(dsess.CurrentBatchModeKey, -9223372036854775808, 9223372036854775807, false),
			Default:           int64(0),
		},
		&sql.MysqlSystemVariable{ // If true, disables the conflict violation check when you commit a transaction.
			Name:              dsess.AllowCommitConflicts,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.AllowCommitConflicts),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.AwsCredsFile,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsFile),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.AwsCredsProfile,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsProfile),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.AwsCredsRegion,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType(dsess.AwsCredsRegion),
			Default:           "",
		},
		&sql.MysqlSystemVariable{
			Name:              dsess.ShowBranchDatabases,
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType(dsess.ShowBranchDatabases),
			Default:           int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltClusterAckWritesTimeoutSecs,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Persist),
			Type:    types.NewSystemIntType(dsess.DoltClusterAckWritesTimeoutSecs, 0, 60, false),
			Default: int64(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.ShowSystemTables,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Type:    types.NewSystemBoolType(dsess.ShowSystemTables),
			Default: int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    "dolt_dont_merge_json",
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
			Type:    types.NewSystemBoolType("dolt_dont_merge_json"),
			Default: int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsAutoRefreshEnabled,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemBoolType(dsess.DoltStatsAutoRefreshEnabled),
			Default: int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsBootstrapEnabled,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemBoolType(dsess.DoltStatsBootstrapEnabled),
			Default: int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsMemoryOnly,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemBoolType(dsess.DoltStatsMemoryOnly),
			Default: int8(0),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsAutoRefreshThreshold,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemDoubleType(dsess.DoltStatsAutoRefreshThreshold, 0, 10),
			Default: float64(.5),
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsAutoRefreshInterval,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemIntType(dsess.DoltStatsAutoRefreshInterval, 0, math.MaxInt, false),
			Default: 120,
		},
		&sql.MysqlSystemVariable{
			Name:    dsess.DoltStatsBranches,
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Type:    types.NewSystemStringType(dsess.DoltStatsBranches),
			Default: "",
		},
		&sql.MysqlSystemVariable{
			Name:    "signingkey",
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_PersistOnly),
			Type:    types.NewSystemStringType("signingkey"),
			Default: "",
		},
		&sql.MysqlSystemVariable{
			Name:    "gpgsign",
			Dynamic: true,
			Scope:   sql.GetMysqlScope(sql.SystemVariableScope_PersistOnly),
			Type:    types.NewSystemBoolType("gpgsign"),
			Default: int8(0),
		},
	})
	sql.SystemVariables.AddSystemVariables(DoltSystemVariables)
}

func ReadReplicaForcePull() bool {
	_, forcePull, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaForcePull)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return forcePull == dsess.SysVarTrue
}
