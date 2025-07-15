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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Per-DB system variables
const (
	HeadKeySuffix          = "_head"
	HeadRefKeySuffix       = "_head_ref"
	WorkingKeySuffix       = "_working"
	StagedKeySuffix        = "_staged"
	DefaultBranchKeySuffix = "_default_branch"
)

// General system variables
const (
	DoltCommitOnTransactionCommit        = "dolt_transaction_commit"
	DoltCommitOnTransactionCommitMessage = "dolt_transaction_commit_message"
	TransactionsDisabledSysVar           = "dolt_transactions_disabled"
	ForceTransactionCommit               = "dolt_force_transaction_commit"
	CurrentBatchModeKey                  = "batch_mode"
	DoltOverrideSchema                   = "dolt_override_schema"
	AllowCommitConflicts                 = "dolt_allow_commit_conflicts"
	ReplicateToRemote                    = "dolt_replicate_to_remote"
	ReadReplicaRemote                    = "dolt_read_replica_remote"
	ReadReplicaForcePull                 = "dolt_read_replica_force_pull"
	ReplicationRemoteURLTemplate         = "dolt_replication_remote_url_template"
	SkipReplicationErrors                = "dolt_skip_replication_errors"
	ReplicateHeads                       = "dolt_replicate_heads"
	ReplicateAllHeads                    = "dolt_replicate_all_heads"
	AsyncReplication                     = "dolt_async_replication"
	AwsCredsFile                         = "aws_credentials_file"
	AwsCredsProfile                      = "aws_credentials_profile"
	AwsCredsRegion                       = "aws_credentials_region"
	ShowBranchDatabases                  = "dolt_show_branch_databases"
	DoltLogLevel                         = "dolt_log_level"
	ShowSystemTables                     = "dolt_show_system_tables"
	AllowCICreation                      = "dolt_allow_ci_creation"

	DoltClusterRoleVariable         = "dolt_cluster_role"
	DoltClusterRoleEpochVariable    = "dolt_cluster_role_epoch"
	DoltClusterAckWritesTimeoutSecs = "dolt_cluster_ack_writes_timeout_secs"

	DoltStatsEnabled     = "dolt_stats_enabled"
	DoltStatsPaused      = "dolt_stats_paused"
	DoltStatsMemoryOnly  = "dolt_stats_memory_only"
	DoltStatsBranches    = "dolt_stats_branches"
	DoltStatsJobInterval = "dolt_stats_job_interval"
	DoltStatsGCInterval  = "dolt_stats_gc_interval"
	DoltStatsGCEnabled   = "dolt_stats_gc_enabled"

	DoltAutoGCEnabled = "dolt_auto_gc_enabled"
)

const URLTemplateDatabasePlaceholder = "{database}"

// DefineSystemVariablesForDB defines per database dolt-session variables in the engine as necessary
func DefineSystemVariablesForDB(name string) {
	name, _ = SplitRevisionDbName(name)

	if _, _, ok := sql.SystemVariables.GetGlobal(name + HeadKeySuffix); !ok {
		sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
			&sql.MysqlSystemVariable{
				Name:              HeadRefKey(name),
				Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              types.NewSystemStringType(HeadRefKey(name)),
				Default:           "",
			},
			// The following variable are Dynamic, but read-only. Their values
			// can only be updates by the system, not by users.
			&sql.MysqlSystemVariable{
				Name:              HeadKey(name),
				Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              types.NewSystemStringType(HeadKey(name)),
				Default:           "",
			},
			&sql.MysqlSystemVariable{
				Name:              WorkingKey(name),
				Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              types.NewSystemStringType(WorkingKey(name)),
				Default:           "",
			},
			&sql.MysqlSystemVariable{
				Name:              StagedKey(name),
				Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Session),
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              types.NewSystemStringType(StagedKey(name)),
				Default:           "",
			},
			&sql.MysqlSystemVariable{
				Name:              DefaultBranchKey(name),
				Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
				Dynamic:           true,
				SetVarHintApplies: false,
				Type:              types.NewSystemStringType(DefaultBranchKey(name)),
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

func DefaultBranchKey(dbName string) string {
	return dbName + DefaultBranchKeySuffix
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

func IsReadOnlyVersionKey(key string) bool {
	return strings.HasSuffix(key, HeadKeySuffix) ||
		strings.HasSuffix(key, StagedKeySuffix) ||
		strings.HasSuffix(key, WorkingKeySuffix)
}

// GetBooleanSystemVar returns a boolean value for the system variable named, returning an error if the variable
// doesn't exist in the session or has a non-boolean type.
func GetBooleanSystemVar(ctx *sql.Context, varName string) (bool, error) {
	val, err := ctx.GetSessionVariable(ctx, varName)
	if err != nil {
		return false, err
	}

	i8, isInt8 := val.(int8)
	if !isInt8 {
		return false, fmt.Errorf("unexpected type for variable %s: %T", varName, val)
	}

	return i8 == int8(1), nil
}

// IgnoreReplicationErrors returns true if the dolt_skip_replication_errors system variable is set to true, which means
// that errors that occur during replication should be logged and ignored.
func IgnoreReplicationErrors() bool {
	_, skip, ok := sql.SystemVariables.GetGlobal(SkipReplicationErrors)
	if !ok {
		panic("dolt system variables not loaded")
	}
	return skip == SysVarTrue
}

// WarnReplicationError logs a warning for the replication error given
func WarnReplicationError(ctx *sql.Context, err error) {
	ctx.GetLogger().Warn(fmt.Errorf("replication failure: %w", err))
}

const (
	SysVarFalse = int8(0)
	SysVarTrue  = int8(1)
)
