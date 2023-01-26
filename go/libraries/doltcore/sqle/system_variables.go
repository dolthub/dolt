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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"

	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

// TODO: get rid of me, use an integration point to define new sysvars
func init() {
	AddDoltSystemVariables()
}

func AddDoltSystemVariables() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
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
			Type:              types.NewSystemStringType(dsess.ReadReplicaForcePull),
			Default:           int8(0),
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
	})
}

func ReadReplicaForcePull() bool {
	_, forcePull, ok := sql.SystemVariables.GetGlobal(dsess.ReadReplicaForcePull)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return forcePull == dsess.SysVarTrue
}
