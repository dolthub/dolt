package doltdb

import (
	"github.com/dolthub/go-mysql-server/sql"
)


const (
	EngineModeKey        = "dolt_engine_mode"
	PermissiveEngineMode = "permissive"
	StrictEngineMode     = "strict"
)

type EngineMode int

const (
	ServerEngineMode = iota
	CliEngineMode
)

const (
	DefaultBranchKey          = "dolt_default_branch"
	ReplicateToRemoteKey  = "dolt_replicate_to_remote"
	ReadReplicaKey           = "dolt_read_replica_remote"
	SkipReplicationErrorsKey = "dolt_skip_replication_errors"
	CurrentBatchModeKey = "batch_mode"
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
			Name:              ReadReplicaKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(ReadReplicaKey),
			Default:           "",
		},
		{
			Name:              SkipReplicationErrorsKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemBoolType(SkipReplicationErrorsKey),
			Default:           false,
		},
		{
			Name:              EngineModeKey,
			Scope:             sql.SystemVariableScope_Global,
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              sql.NewSystemStringType(EngineModeKey),
			Default:           StrictEngineMode,
		},
	})
}