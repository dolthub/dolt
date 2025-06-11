// Copyright 2019 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type ctxKey int
type ctxValue int

const (
	doltCICtxKey ctxKey = iota
)

const (
	doltCICtxValueUnspecified ctxValue = iota
	doltCICtxValueAllow
)

const (
	// DoltNamespace is the name prefix of dolt system tables. We reserve all tables that begin with dolt_ for system use.
	DoltNamespace   = "dolt"
	DoltCINamespace = DoltNamespace + "_ci"
)

var ErrSystemTableCannotBeModified = errors.New("system tables cannot be dropped or altered")

var DocsSchema schema.Schema

func init() {
	docTextCol, err := schema.NewColumnWithTypeInfo(DocTextColumnName, schema.DocTextTag, typeinfo.LongTextType, false, "", false, "")
	if err != nil {
		panic(err)
	}
	doltDocsColumns := schema.NewColCollection(
		schema.NewColumn(DocPkColumnName, schema.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		docTextCol,
	)
	DocsSchema = schema.MustSchemaFromCols(doltDocsColumns)
}

// ContextWithDoltCICreateBypassKey returns the sql.Context with a key that will
// allow dolt_ci tables to be created
func ContextWithDoltCICreateBypassKey(ctx *sql.Context) *sql.Context {
	return ctx.WithContext(context.WithValue(ctx, doltCICtxKey, doltCICtxValueAllow))
}

// DoltCICanBypass checks whether dolt_ci tables can be created
func DoltCICanBypass(ctx context.Context) bool {
	if v := ctx.Value(doltCICtxKey); v != nil {
		if v == doltCICtxValueAllow {
			return true
		}
	}
	return false
}

// HasDoltPrefix returns a boolean whether or not the provided string is prefixed with the DoltNamespace. Users should
// not be able to create tables in this reserved namespace.
func HasDoltPrefix(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), DoltNamespace) && !HasDoltCIPrefix(s)
}

// HasDoltCIPrefix returns a boolean whether or not the provided string is prefixed with the DoltCINamespace. Users should
// not be able to create tables in this reserved namespace.
func HasDoltCIPrefix(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), DoltCINamespace)
}

// IsFullTextTable returns a boolean stating whether the given table is one of the pseudo-index tables used by Full-Text
// indexes.
// TODO: Schema name
func IsFullTextTable(name string) bool {
	return HasDoltPrefix(name) && (strings.HasSuffix(name, "_fts_config") ||
		strings.HasSuffix(name, "_fts_position") ||
		strings.HasSuffix(name, "_fts_doc_count") ||
		strings.HasSuffix(name, "_fts_global_count") ||
		strings.HasSuffix(name, "_fts_row_count"))
}

// IsDoltCITable returns whether the table name given is a dolt-ci table.
func IsDoltCITable(name string) bool {
	return HasDoltCIPrefix(name) && set.NewStrSet(getWriteableSystemTables()).Contains(name) && !IsFullTextTable(name)
}

// IsSystemTable returns whether the table name given is a Dolt system table.
func IsSystemTable(name TableName) bool {
	return HasDoltPrefix(name.Name) || strings.EqualFold(name.Schema, DoltNamespace)
}

// IsReadOnlySystemTable returns whether the table name given is a system table that should not be included in command line
// output (e.g. dolt status) by default.
func IsReadOnlySystemTable(name TableName) bool {
	return IsSystemTable(name) && !set.NewStrSet(getWriteableSystemTables()).Contains(name.Name) && !IsFullTextTable(name.Name)
}

// IsNonAlterableSystemTable returns whether the table name given is a system table that cannot be dropped or altered
// by the user.
func IsNonAlterableSystemTable(name TableName) bool {
	return (IsReadOnlySystemTable(name) && !IsFullTextTable(name.Name)) || strings.EqualFold(name.Name, SchemasTableName)
}

// GetNonSystemTableNames gets non-system table names
func GetNonSystemTableNames(ctx context.Context, root RootValue) ([]string, error) {
	tn, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	tn = funcitr.FilterStrings(tn, func(n string) bool {
		return !HasDoltPrefix(n) && !HasDoltCIPrefix(n)
	})
	sort.Strings(tn)
	return tn, nil
}

// GetSystemTableNames gets system table names
func GetSystemTableNames(ctx context.Context, root RootValue) ([]string, error) {
	p, err := GetPersistedSystemTables(ctx, root)
	if err != nil {
		return nil, err
	}

	g, err := GetGeneratedSystemTables(ctx, root)
	if err != nil {

	}

	s := append(p, g...)
	sort.Strings(s)

	return s, nil
}

// GetPersistedSystemTables returns table names of all persisted system tables.
func GetPersistedSystemTables(ctx context.Context, root RootValue) ([]string, error) {
	tn, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}
	sort.Strings(tn)
	return funcitr.FilterStrings(tn, HasDoltPrefix), nil
}

// GetGeneratedSystemTables returns table names of all generated system tables.
func GetGeneratedSystemTables(ctx context.Context, root RootValue) ([]string, error) {
	s := set.NewStrSet(getGeneratedSystemTables())

	tn, err := root.GetTableNames(ctx, DefaultSchemaName)
	if err != nil {
		return nil, err
	}

	for _, pre := range generatedSystemTablePrefixes {
		s.Add(funcitr.MapStrings(tn, func(s string) string { return pre + s })...)
	}

	return s.AsSlice(), nil
}

// The set of reserved dolt_ tables that should be considered part of user space, like any other user-created table,
// for the purposes of the dolt command line. These tables cannot be created or altered explicitly, but can be updated
// like normal SQL tables.
var getWriteableSystemTables = func() []string {
	return []string{
		GetDocTableName(),
		DoltQueryCatalogTableName,
		SchemasTableName,
		ProceduresTableName,
		IgnoreTableName,
		GetRebaseTableName(),

		// TODO: find way to make these writable by the dolt process
		// TODO: but not by user
		WorkflowsTableName,
		WorkflowEventsTableName,
		WorkflowEventTriggersTableName,
		WorkflowEventTriggerBranchesTableName,
		WorkflowJobsTableName,
		WorkflowStepsTableName,
		WorkflowSavedQueryStepsTableName,
		WorkflowSavedQueryStepExpectedRowColumnResultsTableName,
	}
}

// getGeneratedSystemTables is a function which returns the names of all generated system tables. This is not
// simply a list of constants because doltgres swaps out the functions used to generate different names.
var getGeneratedSystemTables = func() []string {
	return []string{
		GetBranchesTableName(),
		GetRemoteBranchesTableName(),
		GetLogTableName(),
		GetTableOfTablesInConflictName(),
		GetTableOfTablesWithViolationsName(),
		GetCommitsTableName(),
		GetCommitAncestorsTableName(),
		GetStatusTableName(),
		GetRemotesTableName(),
		GetHelpTableName(),
		GetBackupsTableName(),
		GetStashesTableName(),
	}
}

var generatedSystemTablePrefixes = []string{
	DoltDiffTablePrefix,
	DoltCommitDiffTablePrefix,
	DoltHistoryTablePrefix,
	DoltConfTablePrefix,
	DoltConstViolTablePrefix,
	DoltWorkspaceTablePrefix,
}

const (
	// LicenseDoc is the key for accessing the license within the docs table
	LicenseDoc = "LICENSE.md"
	// ReadmeDoc is the key for accessing the readme within the docs table
	ReadmeDoc = "README.md"
)

// GetDocTableName returns the name of the dolt table containing documents such as the license and readme
var GetDocTableName = func() string {
	return DocTableName
}

const (
	// DocTableName is the name of the dolt table containing documents such as the license and readme
	DocTableName = "dolt_docs"
	// DocPkColumnName is the name of the pk column in the docs table
	DocPkColumnName = "doc_name"
	// DocTextColumnName is the name of the column containing the document contents in the docs table
	DocTextColumnName = "doc_text"
)

const (
	// DoltQueryCatalogTableName is the name of the query catalog table
	DoltQueryCatalogTableName = "dolt_query_catalog"

	// QueryCatalogIdCol is the name of the primary key column of the query catalog table
	QueryCatalogIdCol = "id"

	// QueryCatalogOrderCol is the column containing the order of the queries in the catalog
	QueryCatalogOrderCol = "display_order"

	// QueryCatalogNameCol is the name of the column containing the name of a query in the catalog
	QueryCatalogNameCol = "name"

	// QueryCatalogQueryCol is the name of the column containing the query of a catalog entry
	QueryCatalogQueryCol = "query"

	// QueryCatalogDescriptionCol is the name of the column containing the description of a query in the catalog
	QueryCatalogDescriptionCol = "description"
)

const (
	// SchemasTableName is the name of the dolt schema fragment table
	SchemasTableName = "dolt_schemas"
	// SchemasTablesTypeCol is the name of the column that stores the type of a schema fragment  in the dolt_schemas table
	SchemasTablesTypeCol = "type"
	// SchemasTablesNameCol The name of the column that stores the name of a schema fragment in the dolt_schemas table
	SchemasTablesNameCol = "name"
	// SchemasTablesFragmentCol The name of the column that stores the SQL fragment of a schema element in the
	// dolt_schemas table
	SchemasTablesFragmentCol = "fragment"
	// SchemasTablesExtraCol The name of the column that stores extra information about a schema element in the
	// dolt_schemas table
	SchemasTablesExtraCol = "extra"
	// SchemasTablesSqlModeCol is the name of the column that stores the SQL_MODE string used when this fragment
	// was originally defined. Mode settings, such as ANSI_QUOTES, are needed to correctly parse the fragment.
	SchemasTablesSqlModeCol = "sql_mode"
)

const (
	// DoltBlameViewPrefix is the prefix assigned to all the generated blame tables
	DoltBlameViewPrefix = "dolt_blame_"
	// DoltHistoryTablePrefix is the prefix assigned to all the generated history tables
	DoltHistoryTablePrefix = "dolt_history_"
	// DoltDiffTablePrefix is the prefix assigned to all the generated diff tables
	DoltDiffTablePrefix = "dolt_diff_"
	// DoltCommitDiffTablePrefix is the prefix assigned to all the generated commit diff tables
	DoltCommitDiffTablePrefix = "dolt_commit_diff_"
	// DoltConfTablePrefix is the prefix assigned to all the generated conflict tables
	DoltConfTablePrefix = "dolt_conflicts_"
	// DoltConstViolTablePrefix is the prefix assigned to all the generated constraint violation tables
	DoltConstViolTablePrefix = "dolt_constraint_violations_"
	// DoltWorkspaceTablePrefix is the prefix assigned to all the generated workspace tables
	DoltWorkspaceTablePrefix = "dolt_workspace_"
)

// GetBranchesTableName returns the branches system table name
var GetBranchesTableName = func() string {
	return BranchesTableName
}

// GetColumnDiffTableName returns the column diff system table name
var GetColumnDiffTableName = func() string {
	return ColumnDiffTableName
}

// GetCommitAncestorsTableName returns the commit_ancestors system table name
var GetCommitAncestorsTableName = func() string {
	return CommitAncestorsTableName
}

// GetCommitsTableName returns the commits system table name
var GetCommitsTableName = func() string {
	return CommitsTableName
}

// GetTableOfTablesWithViolationsName returns the conflicts system table name
var GetTableOfTablesInConflictName = func() string {
	return TableOfTablesInConflictName
}

// GetTableOfTablesWithViolationsName returns the constraint violations system table name
var GetTableOfTablesWithViolationsName = func() string {
	return TableOfTablesWithViolationsName
}

// GetDiffTableName returns the diff system table name
var GetDiffTableName = func() string {
	return DiffTableName
}

// GetLogTableName returns the log system table name
var GetLogTableName = func() string {
	return LogTableName
}

// GetMergeStatusTableName returns the merge status system table name
var GetMergeStatusTableName = func() string {
	return MergeStatusTableName
}

// GetRebaseTableName returns the rebase system table name
var GetRebaseTableName = func() string {
	return RebaseTableName
}

// GetRemoteBranchesTableName returns the all-branches system table name
var GetRemoteBranchesTableName = func() string {
	return RemoteBranchesTableName
}

// GetRemotesTableName returns the remotes system table name
var GetRemotesTableName = func() string {
	return RemotesTableName
}

// GetSchemaConflictsTableName returns the schema conflicts system table name
var GetSchemaConflictsTableName = func() string {
	return SchemaConflictsTableName
}

// GetStatusTableName returns the status system table name.
var GetStatusTableName = func() string {
	return StatusTableName
}

// GetTagsTableName returns the tags table name
var GetTagsTableName = func() string {
	return TagsTableName
}

// GetHelpTableName returns the help table name
var GetHelpTableName = func() string {
	return HelpTableName
}

var GetBackupsTableName = func() string {
	return BackupsTableName
}

var GetStashesTableName = func() string {
	return StashesTableName
}

const (
	// LogTableName is the log system table name
	LogTableName = "dolt_log"

	// DiffTableName is the name of the table with a map of commits to tables changed
	DiffTableName = "dolt_diff"

	// ColumnDiffTableName is the name of the table with a map of commits to tables and columns changed
	ColumnDiffTableName = "dolt_column_diff"

	// TableOfTablesInConflictName is the conflicts system table name
	TableOfTablesInConflictName = "dolt_conflicts"

	// TableOfTablesWithViolationsName is the constraint violations system table name
	TableOfTablesWithViolationsName = "dolt_constraint_violations"

	// SchemaConflictsTableName is the schema conflicts system table name
	SchemaConflictsTableName = "dolt_schema_conflicts"

	// BranchesTableName is the branches system table name
	BranchesTableName = "dolt_branches"

	// RemoteBranchesTableName is the all-branches system table name
	RemoteBranchesTableName = "dolt_remote_branches"

	// RemotesTableName is the remotes system table name
	RemotesTableName = "dolt_remotes"

	// CommitsTableName is the commits system table name
	CommitsTableName = "dolt_commits"

	// CommitAncestorsTableName is the commit_ancestors system table name
	CommitAncestorsTableName = "dolt_commit_ancestors"

	// StatusTableName is the status system table name.
	StatusTableName = "dolt_status"

	// MergeStatusTableName is the merge status system table name.
	MergeStatusTableName = "dolt_merge_status"

	// TagsTableName is the tags table name
	TagsTableName = "dolt_tags"

	// IgnoreTableName is the ignore table name
	IgnoreTableName = "dolt_ignore"

	// RebaseTableName is the rebase system table name.
	RebaseTableName = "dolt_rebase"

	// StatisticsTableName is the statistics system table name
	StatisticsTableName = "dolt_statistics"

	// StashesTableName is the stashes system table name
	StashesTableName = "dolt_stashes"
)

const (
	// WorkflowsTableName is the dolt CI workflows system table name
	WorkflowsTableName = "dolt_ci_workflows"

	// WorkflowsNameColName is the name of the column storing the name of the workflow.
	WorkflowsNameColName = "name"

	// WorkflowsCreatedAtColName is the name of the column storing the creation time of the row entry.
	WorkflowsCreatedAtColName = "created_at"

	// WorkflowsUpdatedAtColName is the name of the column storing the update time of the row entry.
	WorkflowsUpdatedAtColName = "updated_at"

	// WorkflowEventsTableName is the dolt CI workflow events system table name
	WorkflowEventsTableName = "dolt_ci_workflow_events"

	// WorkflowEventsIdPkColName is the name of the primary key id column on the workflow events table.
	WorkflowEventsIdPkColName = "id"

	// WorkflowEventsWorkflowNameFkColName is the name of the workflows name foreign key in the workflow events table.
	WorkflowEventsWorkflowNameFkColName = "workflow_name_fk"

	// WorkflowEventsEventTypeColName is the name of the event type column in the workflow events table.
	WorkflowEventsEventTypeColName = "event_type"

	// WorkflowEventTriggersTableName is the dolt CI workflow event triggers system table name
	WorkflowEventTriggersTableName = "dolt_ci_workflow_event_triggers"

	// WorkflowEventTriggersIdPkColName is the name of the primary key id column on the workflow event triggers table.
	WorkflowEventTriggersIdPkColName = "id"

	// WorkflowEventTriggersWorkflowEventsIdFkColName is the name of the workflow event id foreign key in the workflow event triggers table.
	WorkflowEventTriggersWorkflowEventsIdFkColName = "workflow_event_id_fk"

	// WorkflowEventTriggerEventTriggerTypeColName is the type of the event trigger on the workflow event triggers table.
	WorkflowEventTriggersEventTriggerTypeColName = "event_trigger_type"

	// WorkflowEventTriggerBranchesTableName is the name of the workflow event trigger branches table name
	WorkflowEventTriggerBranchesTableName = "dolt_ci_workflow_event_trigger_branches"

	// WorkflowEventTriggerActivitiesTableName is then name of a now removed dolt_ci table
	WorkflowEventTriggerActivitiesTableName = "dolt_ci_workflow_event_trigger_activities"

	// WorkflowEventTriggerBranchesIdPkColName is the name of the id column on the workflow event trigger branches table.
	WorkflowEventTriggerBranchesIdPkColName = "id"

	// WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName is the name of the workflow event triggers id foreign key column on the workflow event trigger branches table
	WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName = "workflow_event_triggers_id_fk"

	// WorkflowEventTriggerBranchesBranch is the name of the branch column on the workflow event trigger branches table.
	WorkflowEventTriggerBranchesBranchColName = "branch"

	// WorkflowJobsTableName is the name of the workflow jobs table name
	WorkflowJobsTableName = "dolt_ci_workflow_jobs"

	// WorkflowJobsIdPkColName is the name of the id column on the workflow jobs table
	WorkflowJobsIdPkColName = "id"

	// WorkflowJobsNameColName is the name of the name column on the workflow jobs table
	WorkflowJobsNameColName = "name"

	// WorkflowJobsWorkflowNameFkColName is the name of the workflow name foreign key column name on the workflow jobs table
	WorkflowJobsWorkflowNameFkColName = "workflow_name_fk"

	// WorkflowJobsCreatedAtColName is the name of the created at column on the workflow jobs table
	WorkflowJobsCreatedAtColName = "created_at"

	// WorkflowJobsUpdatedAtColName is the name of the updated at column on the workflow jobs table
	WorkflowJobsUpdatedAtColName = "updated_at"

	// WorkflowStepsTableName is the name of the workflow steps table
	WorkflowStepsTableName = "dolt_ci_workflow_steps"

	// WorkflowStepsIdPkColName is the name of the id column on the workflow steps table
	WorkflowStepsIdPkColName = "id"

	// WorkflowStepsNameColName is the name of the name column on the workflow steps table
	WorkflowStepsNameColName = "name"

	// WorkflowStepsWorkflowJobIdFkColName is the name of the workflow job id foreign key column on the workflow steps table
	WorkflowStepsWorkflowJobIdFkColName = "workflow_job_id_fk"

	// WorkflowStepsStepOrderColName is the name of the step order column on the workflow steps stable
	WorkflowStepsStepOrderColName = "step_order"

	// WorkflowStepsStepTypeColName is the name of the step type column on the workflow steps table
	WorkflowStepsStepTypeColName = "step_type"

	// WorkflowStepsCreatedAtColName is the name of the created at column on the workflow steps table
	WorkflowStepsCreatedAtColName = "created_at"

	// WorkflowStepsUpdatedAtColName is the name of the updated at column on the workflow steps table
	WorkflowStepsUpdatedAtColName = "updated_at"

	// WorkflowSavedQueryStepsTableName is the name of the workflow saved query steps table name
	WorkflowSavedQueryStepsTableName = "dolt_ci_workflow_saved_query_steps"

	// WorkflowSavedQueryStepsIdPkColName is the name of the id column on the workflows saved query steps table
	WorkflowSavedQueryStepsIdPkColName = "id"

	// WorkflowSavedQueryStepsWorkflowStepIdFkColName is the name of the workflow step id foreign key column on the workflow saved query steps table
	WorkflowSavedQueryStepsWorkflowStepIdFkColName = "workflow_step_id_fk"

	// WorkflowSavedQueryStepsSavedQueryNameColName is the name of the saved query name column on the workflow saved query steps table
	WorkflowSavedQueryStepsSavedQueryNameColName = "saved_query_name"

	// WorkflowSavedQueryStepsExpectedResultsTypeColName is the name of the expected results type column on the workflow saved query steps table
	WorkflowSavedQueryStepsExpectedResultsTypeColName = "expected_results_type"

	// WorkflowSavedQueryStepExpectedRowColumnResultsTableName is the name of the saved query step expected row column results table name
	WorkflowSavedQueryStepExpectedRowColumnResultsTableName = "dolt_ci_workflow_saved_query_step_expected_row_column_results"

	// WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName is the name of the id column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsIdPkColName = "id"

	// WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName is the name of the workflow saved query step id foreign key column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName = "workflow_saved_query_step_id_fk"

	// WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName is the name of the expected column count comparison type column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName = "expected_column_count_comparison_type"

	// WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName is the name of the expected row count comparison type column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName = "expected_column_row_comparison_type"

	// WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName is the name of the expected column count column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName = "expected_column_count"

	// WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName is the name of the expected row count column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName = "expected_row_count"

	// WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName is the name of the created at column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsCreatedAtColName = "created_at"

	// WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName is the name of the updated at column on the workflow saved query step expected row column results table
	WorkflowSavedQueryStepExpectedRowColumnResultsUpdatedAtColName = "updated_at"
)

const (
	// ProceduresTableName is the name of the dolt stored procedures table.
	ProceduresTableName = "dolt_procedures"
	// ProceduresTableNameCol is the name of the stored procedure. Using CREATE PROCEDURE, will always be lowercase.
	ProceduresTableNameCol = "name"
	// ProceduresTableCreateStmtCol is the CREATE PROCEDURE statement for this stored procedure.
	ProceduresTableCreateStmtCol = "create_stmt"
	// ProceduresTableCreatedAtCol is the time that the stored procedure was created at, in UTC.
	ProceduresTableCreatedAtCol = "created_at"
	// ProceduresTableModifiedAtCol is the time that the stored procedure was last modified, in UTC.
	ProceduresTableModifiedAtCol = "modified_at"
	// ProceduresTableSqlModeCol is the name of the column that stores the SQL_MODE string used when this fragment
	// was originally defined. Mode settings, such as ANSI_QUOTES, are needed to correctly parse the fragment.
	ProceduresTableSqlModeCol = "sql_mode"
)

const (
	HelpTableName    = "dolt_help"
	BackupsTableName = "dolt_backups"
)
