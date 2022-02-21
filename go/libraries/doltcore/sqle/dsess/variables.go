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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

const (
	HeadKeySuffix    = "_head"
	HeadRefKeySuffix = "_head_ref"
	WorkingKeySuffix = "_working"
	StagedKeySuffix  = "_staged"
)

const (
	DoltCommitOnTransactionCommit = "dolt_transaction_commit"
	TransactionsDisabledSysVar    = "dolt_transactions_disabled"
	ForceTransactionCommit        = "dolt_force_transaction_commit"
	CurrentBatchModeKey           = "batch_mode"
	AllowCommitConflicts          = "dolt_allow_commit_conflicts"
)

func init() {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		{ // If true, causes a Dolt commit to occur when you commit a transaction.
			Name:              DoltCommitOnTransactionCommit,
			Scope:             sql.SystemVariableScope_Both,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(DoltCommitOnTransactionCommit),
			Default:           int8(0),
		},
		{
			Name:              TransactionsDisabledSysVar,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(TransactionsDisabledSysVar),
			Default:           int8(0),
		},
		{ // If true, disables the conflict and constraint violation check when you commit a transaction.
			Name:              ForceTransactionCommit,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(ForceTransactionCommit),
			Default:           int8(0),
		},
		{
			Name:              CurrentBatchModeKey,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemIntType(CurrentBatchModeKey, -9223372036854775808, 9223372036854775807, false),
			Default:           int64(0),
		},
		{ // If true, disables the conflict violation check when you commit a transaction.
			Name:              AllowCommitConflicts,
			Scope:             sql.SystemVariableScope_Session,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(AllowCommitConflicts),
			Default:           int8(1),
		},
	})
}

// defineSystemVariables defines dolt-session variables in the engine as necessary
func defineSystemVariables(name string) {
	if _, _, ok := sql.SystemVariables.GetGlobal(name + HeadKeySuffix); !ok {
		sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
			{
				Name:              HeadRefKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(HeadRefKey(name)),
				Default:           "",
			},
			{
				Name:              HeadKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(HeadKey(name)),
				Default:           "",
			},
			{
				Name:              WorkingKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(WorkingKey(name)),
				Default:           "",
			},
			{
				Name:              StagedKey(name),
				Scope:             sql.SystemVariableScope_Session,
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              sql.NewSystemStringType(StagedKey(name)),
				Default:           "",
			},
		})
	}
}

func HeadKey(dbName string) string {
	return dbName + HeadKeySuffix
}

func HeadRefKey(dbName string) string {
	return dbName + HeadRefKeySuffix
}

func WorkingKey(dbName string) string {
	return dbName + WorkingKeySuffix
}

func StagedKey(dbName string) string {
	return dbName + StagedKeySuffix
}

func IsHeadKey(key string) (bool, string) {
	if strings.HasSuffix(key, HeadKeySuffix) {
		return true, key[:len(key)-len(HeadKeySuffix)]
	}

	return false, ""
}

func IsHeadRefKey(key string) (bool, string) {
	if strings.HasSuffix(key, HeadRefKeySuffix) {
		return true, key[:len(key)-len(HeadRefKeySuffix)]
	}

	return false, ""
}

func IsWorkingKey(key string) (bool, string) {
	if strings.HasSuffix(key, WorkingKeySuffix) {
		return true, key[:len(key)-len(WorkingKeySuffix)]
	}

	return false, ""
}
