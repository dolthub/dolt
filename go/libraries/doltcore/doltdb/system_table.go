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

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type ctxKey int
type ctxValue int

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

// HasDoltPrefix returns a boolean whether or not the provided string is prefixed with the DoltNamespace. Users should
// not be able to create tables in this reserved namespace.
func HasDoltPrefix(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), DoltNamespace)
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
	tn, err := root.GetTableNames(ctx, DefaultSchemaName, true)
	if err != nil {
		return nil, err
	}
	tn = funcitr.FilterStrings(tn, func(n string) bool {
		return !HasDoltPrefix(n) && !HasDoltCIPrefix(n)
	})
	sort.Strings(tn)
	return tn, nil
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
		GetQueryCatalogTableName(),
		GetTestsTableName(),

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

// GeneratedSystemTableNames returns the names of all generated system tables. This is not
// simply a list of constants because doltgres swaps out the functions used to generate different names.
func GeneratedSystemTableNames() []string {
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

// GeneratedSystemTablePrefixes returns the prefixes of all generated system tables, the ones that exist for every
// user table.
var GeneratedSystemTablePrefixes = []string{
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
	// AgentDoc is the key for accessing the agent documentation within the docs table
	AgentDoc = "AGENT.md"

	DefaultAgentDocValue = `# AGENT.md - Dolt Database Operations Guide

This file provides guidance for AI agents working with Dolt databases to maximize productivity and follow best practices.

## Quick Start

Dolt is "Git for Data" - a SQL database with version control capabilities. All Git commands have Dolt equivalents:
- ` + "`git add` → `dolt add`" + `  
- ` + "`git commit` → `dolt commit`" + `
- ` + "`git branch` → `dolt branch`" + `
- ` + "`git merge` → `dolt merge`" + `
- ` + "`git diff` → `dolt diff`" + `

For help and documentation on commands, you can run ` + "`dolt --help`" + ` and ` + "`dolt <command> --help`" + `.

## Essential Dolt CLI Commands

### Repository Operations
` + "```bash" + `
# Initialize new database
dolt init

# Clone existing database
dolt clone <remote-url>

# Show current status
dolt status

# View commit history
dolt log
` + "```" + `

### Branch Management
` + "```bash" + `
# List branches
dolt branch

# Create new branch
dolt branch <branch-name>

# Switch branches
dolt checkout <branch-name>

# Create and switch to new branch
dolt checkout -b <branch-name>
` + "```" + `

### Data Operations
` + "```bash" + `
# Stage changes
dolt add <table-name>
dolt add .  # stage all changes

# Commit changes
dolt commit -m "commit message"

# View differences
dolt diff
dolt diff <table-name>
dolt diff <branch1> <branch2>

# Merge branches
dolt merge <branch-name>
` + "```" + `

## Starting and Connecting to Dolt SQL Server

### Start SQL Server
` + "```bash" + `
# Start server on default port (3306)
dolt sql-server

# Start on specific port
dolt sql-server --port=3307

# Start with specific host
dolt sql-server --host=0.0.0.0 --port=3307

# Start in background
dolt sql-server --port=3307 &
` + "```" + `

### Connecting to SQL Server
` + "```bash" + `
# Connect with dolt sql command
dolt sql

# Connect with mysql client
mysql -h 127.0.0.1 -P 3306 -u root

# Connect with specific database
mysql -h 127.0.0.1 -P 3306 -u root -D <database-name>
` + "```" + `

## Dolt Testing with dolt_test System Table

### Unit Testing with dolt_test

The dolt_test system table provides a powerful way to create and run unit tests for your database. This is the preferred method for testing data integrity, business rules, and schema validation.

#### Creating Tests

Tests are created by inserting rows into the dolt_tests system table:

` + "```sql" + `
-- Create a simple test
INSERT INTO dolt_tests VALUES (
    'test_user_count', 
    'validation', 
    'SELECT COUNT(*) as user_count FROM users;', 
    'row_count',
    '>',
    '0'
);

-- Create a test with expected result
INSERT INTO dolt_tests VALUES (
    'test_valid_emails', 
    'validation', 
    'SELECT COUNT(*) FROM users WHERE email NOT LIKE "%@%";', 
    'row_count',
    '==',
    '0'
);

-- Create a schema validation test
INSERT INTO dolt_tests VALUES (
    'test_users_schema', 
    'schema', 
    'DESCRIBE users;', 
    'row_count',
    '>=',
    '5'
);
` + "```" + `

#### Test Structure

Each test row contains:
- test_name: Unique identifier for the test
- test_group: Optional grouping for tests (e.g., 'validation', 'schema', 'integration')
- test_query: SQL query to execute
- assertion_type: Type of assertion ('expected_rows', 'expected_columns', 'expected_single_value', etc.)
- assertion_comparator: Comparison operator ('==', '>', '<', '>=', '<=', '!=')
- assertion_value: Expected value for comparison

#### Running Tests

` + "```sql" + `
-- Run all tests
SELECT * FROM dolt_test_run();

-- Run specific test
SELECT * FROM dolt_test_run('test_user_count');

-- Run tests with filtering
SELECT * FROM dolt_test_run() WHERE test_name LIKE 'test_user%';
` + "```" + `

#### Test Result Interpretation

The dolt_test_run() function returns:
- test_name: Name of the test
- status: PASS, FAIL, or ERROR
- actual_result: Actual query result
- expected_result: Expected result
- message: Additional details

#### Advanced Testing Examples

` + "```sql" + `
-- Test data integrity
INSERT INTO dolt_tests VALUES (
    'test_no_orphaned_orders', 
    'integrity', 
    'SELECT COUNT(*) FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE u.id IS NULL;', 
    'row_count',
    '==',
    '0'
);

-- Test business rules
INSERT INTO dolt_tests VALUES (
    'test_positive_prices', 
    'business_rules', 
    'SELECT COUNT(*) FROM products WHERE price <= 0;', 
    'row_count',
    '==',
    '0'
);

-- Test complex relationships
INSERT INTO dolt_tests VALUES (
    'test_order_totals', 
    'integrity', 
    'SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.id HAVING SUM(oi.quantity * oi.price) != o.total;', 
    'row_count',
    '==',
    '0'
);
` + "```" + `

### Dolt CI for DoltHub Integration

Dolt CI is specifically designed for running tests on DoltHub when pull requests are created. Use this only for tests you want to run automatically on DoltHub.

#### Prerequisites for DoltHub CI
- Requires Dolt v1.43.14 or later
- Must initialize CI capabilities: ` + "`dolt ci init`" + `
- Workflows defined in YAML files

#### Available CI Commands
` + "```bash" + `
# Initialize CI capabilities
dolt ci init

# List available workflows
dolt ci ls

# View workflow details
dolt ci view <workflow-name>

# View specific job in workflow
dolt ci view <workflow-name> <job-name>

# Run workflow locally (for testing before DoltHub)
dolt ci run <workflow-name>
` + "```" + `

#### Creating CI Workflows for DoltHub

Create workflow files that will run on DoltHub when pull requests are opened:

` + "```yaml" + `
name: doltHub validation workflow
on:
  push:
    branches:
      - master
      - main
jobs:
  - name: validate schema
    steps:
      - name: check required tables exist
        saved_query_name: show_tables
        expected_rows: ">= 3"
      
      - name: validate user data
        saved_query_name: user_count_check
        expected_columns: "== 1"
        expected_rows: "> 0"
  
  - name: data integrity checks
    steps:
      - name: check email format
        saved_query_name: valid_emails
        expected_rows: "== 0"  # No invalid emails
` + "```" + `

### Best Practices for Testing

1. **Use dolt_test for Unit Testing**
   - Create tests for data validation
   - Test business rules and constraints
   - Validate schema changes
   - Run tests frequently during development

2. **Use Dolt CI for DoltHub Integration**
   - Only for tests that should run on pull requests
   - Focus on integration and deployment validation
   - Test against production-like data

3. **Create Comprehensive Test Suites**
   - Test data integrity constraints
   - Validate business rules
   - Check schema requirements
   - Verify data relationships

4. **Version Control Your Tests**
   - Commit test definitions to repository
   - Track changes to test configuration
   - Use branches for test development

## System Tables for Version Control

Dolt exposes version control operations through system tables accessible via SQL:

### Core System Tables
` + "```sql" + `
-- View commit history
SELECT * FROM dolt_log;

-- Check current status
SELECT * FROM dolt_status;

-- View branch information
SELECT * FROM dolt_branches;

-- See table diffs
SELECT * FROM dolt_diff_<table_name>;

-- View schema changes
SELECT * FROM dolt_schema_diff;

-- Check conflicts during merge
SELECT * FROM dolt_conflicts_<table_name>;

-- View commit metadata
SELECT * FROM dolt_commits;
` + "```" + `

### Version Control Operations via SQL

When working in SQL sessions, you can execute version control operations using stored procedures:

` + "```sql" + `
-- Stage and commit changes
CALL dolt_add('.');
CALL dolt_commit('-m', 'commit message');

-- Branch operations
CALL dolt_branch('<branch_name>');
CALL dolt_checkout('<branch_name>');
CALL dolt_merge('<branch_name>');
` + "```" + `

**Note:** Use CLI commands (` + "`dolt add`, `dolt commit`, etc." + `) for most operations. SQL procedures are useful when already in a SQL session.

### Advanced System Tables
` + "```sql" + `
-- View remotes
SELECT * FROM dolt_remotes;

-- Check merge conflicts
SELECT * FROM dolt_conflicts;

-- View statistics
SELECT * FROM dolt_statistics;

-- See ignored tables
SELECT * FROM dolt_ignore;
` + "```" + `

## CLI vs SQL Approach

**Prefer CLI commands for:**
- Version control operations (add, commit, branch, merge)
- Repository management (init, clone, push, pull)
- Conflict resolution
- Status checking and history viewing

**Use SQL for:**
- Data queries and analysis
- Complex data transformations
- Examining system tables (dolt_log, dolt_status, etc.)
- When already in an active SQL session

## Schema Design Recommendations

### Use UUID Keys Instead of Auto-Increment

For Dolt's version control features, use UUID primary keys instead of auto-increment:

` + "```sql" + `
-- Recommended
CREATE TABLE users (
    id varchar(36) default(uuid()) primary key,
    name varchar(255)
);

-- Avoid auto-increment with Dolt
-- id int auto_increment primary key
` + "```" + `

**Benefits:**
- Prevents merge conflicts across branches and database clones
- Automatic generation with default(uuid())
- Works seamlessly in distributed environments

## Best Practices for Agents

### 1. Always Work on Feature Branches
` + "```bash" + `
# Create feature branch before making changes
dolt checkout -b feature/agent-changes

# Make changes on feature branch
dolt sql -q "INSERT INTO users VALUES (1, 'Alice');"

# Stage and commit
dolt add .
dolt commit -m "Add new user Alice"

# Switch back to main to merge
dolt checkout main
dolt merge feature/agent-changes
` + "```" + `

### 2. Use SQL for Data Operations, CLI for Version Control
` + "```bash" + `
# Use dolt sql for data changes
dolt sql -q "INSERT INTO users VALUES (1, 'Alice');"
dolt sql -q "UPDATE products SET price = price * 1.1 WHERE category = 'electronics';"

# Check status and commit using CLI
dolt status
dolt add .
dolt commit -m "Update user and product data"
` + "```" + `

### 3. Validate Changes with System Tables
` + "```sql" + `
-- Before major operations, check current state
SELECT * FROM dolt_status;
SELECT * FROM dolt_branches;

-- After changes, verify with diffs
SELECT * FROM dolt_diff_users;
SELECT * FROM dolt_schema_diff;
` + "```" + `

### 4. Use dolt_test for Data Validation
Create tests to validate:
- Data integrity after changes
- Schema compatibility
- Business rule compliance
- Cross-table relationships

### 5. Handle Conflicts Gracefully
` + "```bash" + `
# Check for conflicts using CLI
dolt conflicts cat <table_name>
dolt conflicts resolve --ours <table_name>
dolt conflicts resolve --theirs <table_name>

# Or use SQL to examine conflicts
dolt sql -q "SELECT * FROM dolt_conflicts_<table_name>;"
` + "```" + `

## Common Workflow Examples

### Data Migration Workflow
` + "```bash" + `
# Create migration branch
dolt checkout -b migration/update-schema

# Apply schema changes via SQL
dolt sql -q "ALTER TABLE users ADD COLUMN email VARCHAR(255);"

# Create validation tests
dolt sql -q "INSERT INTO dolt_tests VALUES ('test_users_schema', 'schema', 'DESCRIBE users;', 'row_count', '>=', '6');"
dolt sql -q "INSERT INTO dolt_tests VALUES ('test_email_column', 'schema', 'SELECT COUNT(*) FROM users WHERE email IS NULL;', 'row_count', '>=', '0');"

# Run tests to validate changes
dolt sql -q "SELECT * FROM dolt_test_run();"

# Stage and commit
dolt add .
dolt commit -m "Add email column to users table"

# Merge back
dolt checkout main
dolt merge migration/update-schema
` + "```" + `

### Data Analysis Workflow
` + "```bash" + `
# Create analysis branch
dolt checkout -b analysis/user-behavior

# Create analysis tables via SQL
dolt sql -q "CREATE TABLE user_metrics AS 
            SELECT user_id, COUNT(*) as actions 
            FROM user_actions 
            GROUP BY user_id;"

# Create tests to validate analysis
dolt sql -q "INSERT INTO dolt_tests VALUES ('test_metrics_created', 'analysis', 'SELECT COUNT(*) FROM user_metrics;', 'row_count', '>', '0');"
dolt sql -q "INSERT INTO dolt_tests VALUES ('test_metrics_integrity', 'integrity', 'SELECT COUNT(*) FROM user_metrics um LEFT JOIN users u ON um.user_id = u.id WHERE u.id IS NULL;', 'row_count', '==', '0');"

# Run tests to validate analysis
dolt sql -q "SELECT * FROM dolt_test_run();"

# Stage and commit using CLI
dolt add user_metrics
dolt commit -m "Add user behavior analysis"
` + "```" + `

## Integration with External Tools

### Database Clients
Most MySQL clients work with Dolt:
- MySQL Workbench
- phpMyAdmin  
- DataGrip
- DBeaver

### Backup and Sync
` + "```bash" + `
# Push to remote
dolt push origin main

# Pull changes
dolt pull origin main

# Clone for backup
dolt clone <remote-url> backup-location
` + "```" + `

This guide enables agents to leverage Dolt's unique version control capabilities while maintaining data integrity and following collaborative development practices.`
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

	// DiffTypeCol is the column name for the type of change (added, modified, removed) in diff tables
	DiffTypeCol = "diff_type"

	// ToCommitCol is the column name for the "to" commit in diff tables
	ToCommitCol = "to_commit"

	// FromCommitCol is the column name for the "from" commit in diff tables
	FromCommitCol = "from_commit"

	// ToCommitDateCol is the column name for the "to" commit date in diff tables
	ToCommitDateCol = "to_commit_date"

	// FromCommitDateCol is the column name for the "from" commit date in diff tables
	FromCommitDateCol = "from_commit_date"

	// WorkingCommitRef is the special commit reference for working changes
	WorkingCommitRef = "WORKING"

	// EmptyCommitRef is the special commit reference for empty/initial state
	EmptyCommitRef = "EMPTY"

	// DiffTypeAdded represents a row that was added in a diff
	DiffTypeAdded = "added"

	// DiffTypeModified represents a row that was modified in a diff
	DiffTypeModified = "modified"

	// DiffTypeRemoved represents a row that was removed in a diff
	DiffTypeRemoved = "removed"

	// DiffToPrefix is the prefix for "to" columns in diff tables
	DiffToPrefix = "to_"

	// DiffFromPrefix is the prefix for "from" columns in diff tables
	DiffFromPrefix = "from_"
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

var GetQueryCatalogTableName = func() string { return DoltQueryCatalogTableName }

var GetTestsTableName = func() string {
	return TestsTableName
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

	// TestsTableName is the tests system table name
	TestsTableName = "dolt_tests"
)

// DoltGeneratedTableNames is a list of all the generated dolt system tables that are not specific to a user table.
// It does not include tables in the dolt_ prefix namespace that are user-populated, such as dolt_ignore or dolt_docs.
// These tables are addressable by these names in both Dolt and Doltgres. In doltgres they are additionally addressable
// in the `dolt.` schema, e.g. `dolt.branches`
var DoltGeneratedTableNames = []string{
	LogTableName,
	DiffTableName,
	ColumnDiffTableName,
	TableOfTablesInConflictName,
	TableOfTablesWithViolationsName,
	SchemaConflictsTableName,
	BranchesTableName,
	RemoteBranchesTableName,
	RemotesTableName,
	CommitsTableName,
	CommitAncestorsTableName,
	StatusTableName,
	MergeStatusTableName,
	TagsTableName,
}

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
	WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName = "expected_row_count_comparison_type"

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
