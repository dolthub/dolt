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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/store/datas"
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

	DoltLogCommitterOnly = "dolt_log_committer_only"

	DoltAuthorName     = "dolt_author_name"
	DoltAuthorEmail    = "dolt_author_email"
	DoltAuthorDate     = "dolt_author_date"
	DoltCommitterName  = "dolt_committer_name"
	DoltCommitterEmail = "dolt_committer_email"
	DoltCommitterDate  = "dolt_committer_date"
)

const URLTemplateDatabasePlaceholder = "{database}"

// DefineSystemVariablesForDB defines per database dolt-session variables in the engine as necessary
func DefineSystemVariablesForDB(name string) {
	name, _ = doltdb.SplitRevisionDbName(name)

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

// NewCommitStagedProps creates an [actions.CommitStagedProps] using |message| as the commit
// message and resolving author and committer identity via [ResolveNameEmail], which walks
// session variables, the SQL client ([sql.Client]), then the session's configured identity.
// The returned error surfaces session-variable access or date parse failures; the
// empty-identity check happens downstream in [datas.NewCommitMetaWithAuthorCommitter].
func NewCommitStagedProps(ctx *sql.Context, message string) (actions.CommitStagedProps, error) {
	authorName, authorEmail, err := ResolveNameEmail(ctx, DoltAuthorName, DoltAuthorEmail)
	if err != nil {
		return actions.CommitStagedProps{}, err
	}
	authorDate, err := resolveDate(ctx, DoltAuthorDate)
	if err != nil {
		return actions.CommitStagedProps{}, err
	}

	committerName, committerEmail, err := ResolveNameEmail(ctx, DoltCommitterName, DoltCommitterEmail)
	if err != nil {
		return actions.CommitStagedProps{}, err
	}
	committerDate, err := resolveDate(ctx, DoltCommitterDate)
	if err != nil {
		return actions.CommitStagedProps{}, err
	}

	return actions.CommitStagedProps{
		Message:   message,
		Author:    datas.CommitIdent{Name: authorName, Email: authorEmail, Date: authorDate},
		Committer: datas.CommitIdent{Name: committerName, Email: committerEmail, Date: committerDate},
	}, nil
}

// ResolveNameEmail reads the name and email session variables for a single author or committer,
// falling back to the SQL client user and address when the variables are unset, and to the
// session's configured identity when the session has no wire client.
func ResolveNameEmail(ctx *sql.Context, nameVar, emailVar string) (string, string, error) {
	name, err := systemVarString(ctx, nameVar)
	if err != nil {
		return "", "", err
	}
	email, err := systemVarString(ctx, emailVar)
	if err != nil {
		return "", "", err
	}

	client := ctx.Client()
	if name == "" {
		if client.User != "" {
			name = client.User
		} else {
			// No wire client on this session, so it was constructed internally
			// by the server (background workers, cluster replication, tests).
			// Fall back to the server's configured identity.
			name = DSessFromSess(ctx.Session).Username()
		}
	}
	if email == "" {
		if client.User != "" {
			email = fmt.Sprintf("%s@%s", client.User, client.Address)
		} else {
			email = DSessFromSess(ctx.Session).Email()
		}
	}

	return name, email, nil
}

// resolveDate reads the date session variable for a single author or committer,
// returning the unset [datas.CommitDate] zero value when the variable is unset or empty.
func resolveDate(ctx *sql.Context, dateVar string) (datas.CommitDate, error) {
	strVal, err := systemVarString(ctx, dateVar)
	if err != nil || strVal == "" {
		return datas.CommitDate{}, err
	}
	return datas.NewCommitDate(strVal)
}

// systemVarString returns the string value of the named system variable, returning an empty string
// when the variable is unset.
func systemVarString(ctx *sql.Context, varName string) (string, error) {
	val, err := ctx.GetSessionVariable(ctx, varName)
	if err != nil {
		return "", err
	}
	strVal, _ := val.(string)
	return strVal, nil
}
