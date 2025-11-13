# AGENT.md - Dolt Database Operations Guide

This file provides guidance for AI agents working with Dolt databases to maximize productivity and follow best practices.

## Quick Start

Dolt is "Git for Data" - a SQL database with version control capabilities. All Git commands have Dolt equivalents:
- `git add` → `dolt add`  
- `git commit` → `dolt commit`
- `git branch` → `dolt branch`
- `git merge` → `dolt merge`
- `git diff` → `dolt diff`

For help and documentation on commands, you can run `dolt --help` and `dolt <command> --help`.

## Essential Dolt CLI Commands

### Repository Operations
```bash
# Initialize new database
dolt init

# Clone existing database
dolt clone <remote-url>

# Show current status
dolt status

# View commit history
dolt log
```

### Branch Management
```bash
# List branches
dolt branch

# Create new branch
dolt branch <branch-name>

# Switch branches
dolt checkout <branch-name>

# Create and switch to new branch
dolt checkout -b <branch-name>
```

### Checkout Behavior with Running SQL Servers
- `dolt checkout` on the CLI only affects the shell process that runs the command. When a `dolt sql-server` is running, existing SQL connections keep their current branch until they explicitly switch.
- Each SQL session (CLI `dolt sql`, MySQL client, application connection) maintains its own active branch. Run `CALL dolt_checkout('<branch>');` at the beginning of every session or scripted block to ensure you are on the correct branch.
- Chain branch changes inside scripts: start with `CALL dolt_checkout('<branch>');`, then run your queries. Do not assume a previous checkout persists for new connections.
- When automating, include the checkout in the same transaction / session context where the data changes execute.

### Data Operations
```bash
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
```

## Starting and Connecting to Dolt SQL Server

### Start SQL Server
```bash
# Start server on default port (3306)
dolt sql-server

# Start on specific port
dolt sql-server --port=3307

# Start with specific host
dolt sql-server --host=0.0.0.0 --port=3307

# Start in background
dolt sql-server --port=3307 &
```

### Connecting to SQL Server
```bash
# Connect with dolt sql command
dolt sql

# Connect with mysql client
mysql -h 127.0.0.1 -P 3306 -u root

# Connect with specific database
mysql -h 127.0.0.1 -P 3306 -u root -D <database-name>
```

## Dolt Testing with dolt_test System Table

### Unit Testing with dolt_test

The dolt_test system table provides a powerful way to create and run unit tests for your database. This is the preferred method for testing data integrity, business rules, and schema validation.

#### Creating Tests

Tests are created by inserting rows into the `dolt_tests` system table:

```sql
-- Create a simple test
INSERT INTO `dolt_tests` VALUES (
    'test_user_count', 
    'validation', 
    'SELECT COUNT(*) as user_count FROM users;', 
    'row_count',
    '>',
    '0'
);

-- Create a test with expected result
INSERT INTO `dolt_tests` VALUES (
    'test_valid_emails', 
    'validation', 
    'SELECT COUNT(*) FROM users WHERE email NOT LIKE "%@%";', 
    'row_count',
    '==',
    '0'
);

-- Create a schema validation test
INSERT INTO `dolt_tests` VALUES (
    'test_users_schema', 
    'schema', 
    'DESCRIBE users;', 
    'row_count',
    '>=',
    '5'
);
```

#### Test Structure

Each test row contains:
- test_name: Unique identifier for the test
- test_group: Optional grouping for tests (e.g., 'validation', 'schema', 'integration')
- test_query: SQL query to execute
- assertion_type: Type of assertion ('expected_rows', 'expected_columns', 'expected_single_value')
- assertion_comparator: Comparison operator ('==', '>', '<', '>=', '<=', '!=')
- assertion_value: Expected value for comparison

#### Running Tests

```sql
-- Run all tests
SELECT * FROM dolt_test_run();

-- Run specific test
SELECT * FROM dolt_test_run('test_user_count');

-- Run tests with filtering
SELECT * FROM dolt_test_run() WHERE test_name LIKE 'test_user%' AND status != 'PASS';
```

#### Test Result Interpretation

The dolt_test_run() function returns:
- test_name: Name of the test
- status: PASS, FAIL, or ERROR
- actual_result: Actual query result
- expected_result: Expected result
- message: Additional details

#### Advanced Testing Examples

```sql
-- Test data integrity
INSERT INTO `dolt_tests` VALUES (
    'test_no_orphaned_orders', 
    'integrity', 
    'SELECT COUNT(*) FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE u.id IS NULL;', 
    'row_count',
    '==',
    '0'
);

-- Test business rules
INSERT INTO `dolt_tests` VALUES (
    'test_positive_prices', 
    'business_rules', 
    'SELECT COUNT(*) FROM products WHERE price <= 0;', 
    'row_count',
    '==',
    '0'
);

-- Test complex relationships
INSERT INTO `dolt_tests` VALUES (
    'test_order_totals', 
    'integrity', 
    'SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.id HAVING SUM(oi.quantity * oi.price) != o.total;', 
    'row_count',
    '==',
    '0'
);
```

### Dolt CI for DoltHub Integration

Dolt CI is specifically designed for running tests on DoltHub when pull requests are created. Use this only for tests you want to run automatically on DoltHub.

#### Prerequisites for DoltHub CI
- Requires Dolt v1.43.14 or later
- Must initialize CI capabilities: `dolt ci init`
- Workflows defined in YAML files

#### Available CI Commands
```bash
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
```

#### Creating CI Workflows for DoltHub

Create workflow files that will run on DoltHub when pull requests are opened:

```yaml
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
```

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
```sql
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
```

### Version Control Operations via SQL

When working in SQL sessions, you can execute version control operations using stored procedures:

```sql
-- Stage and commit changes
CALL dolt_add('.');
CALL dolt_commit('-m', 'commit message');

-- Branch operations
CALL dolt_branch('<branch_name>');
CALL dolt_checkout('<branch_name>');
CALL dolt_merge('<branch_name>');
```

**Note:** Use CLI commands (`dolt add`, `dolt commit`, etc.) for most operations. SQL procedures are useful when already in a SQL session.

### Advanced System Tables
```sql
-- View remotes
SELECT * FROM dolt_remotes;

-- Check merge conflicts
SELECT * FROM dolt_conflicts;

-- View statistics
SELECT * FROM dolt_statistics;

-- See ignored tables
SELECT * FROM dolt_ignore;
```

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

```sql
-- Recommended
CREATE TABLE users (
    id varchar(36) default(uuid()) primary key,
    name varchar(255)
);

-- Avoid auto-increment with Dolt
-- id int auto_increment primary key
```

**Benefits:**
- Prevents merge conflicts across branches and database clones
- Automatic generation with default(uuid())
- Works seamlessly in distributed environments

## Best Practices for Agents

### 1. Always Work on Feature Branches
```bash
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
```

### 2. Use SQL for Data Operations, CLI for Version Control
```bash
# Use dolt sql for data changes
dolt sql -q "INSERT INTO users VALUES (1, 'Alice');"
dolt sql -q "UPDATE products SET price = price * 1.1 WHERE category = 'electronics';"

# Check status and commit using CLI
dolt status
dolt add .
dolt commit -m "Update user and product data"
```

### 3. Validate Changes with System Tables
```sql
-- Before major operations, check current state
SELECT * FROM dolt_status;
SELECT * FROM dolt_branches;

-- After changes, verify with diffs
SELECT * FROM dolt_diff_users;
SELECT * FROM dolt_schema_diff;
```

### 4. Use dolt_test for Data Validation
Create tests to validate:
- Data integrity after changes
- Schema compatibility
- Business rule compliance
- Cross-table relationships

### 5. Handle Conflicts Gracefully
```bash
# Check for conflicts using CLI
dolt conflicts cat <table_name>
dolt conflicts resolve --ours <table_name>
dolt conflicts resolve --theirs <table_name>

# Or use SQL to examine conflicts
dolt sql -q "SELECT * FROM dolt_conflicts_<table_name>;"
```

## Common Workflow Examples

### Data Migration Workflow
```bash
# Create migration branch
dolt checkout -b migration/update-schema

# Apply schema changes via SQL
dolt sql -q "ALTER TABLE users ADD COLUMN email VARCHAR(255);"

# Create validation tests
dolt sql -q "INSERT INTO `dolt_tests` VALUES ('test_users_schema', 'schema', 'DESCRIBE users;', 'row_count', '>=', '6');"
dolt sql -q "INSERT INTO `dolt_tests` VALUES ('test_email_column', 'schema', 'SELECT COUNT(*) FROM users WHERE email IS NULL;', 'row_count', '>=', '0');"

# Run tests to validate changes
dolt sql -q "SELECT * FROM dolt_test_run();"

# Stage and commit
dolt add .
dolt commit -m "Add email column to users table"

# Merge back
dolt checkout main
dolt merge migration/update-schema
```

### Data Analysis Workflow
```bash
# Create analysis branch
dolt checkout -b analysis/user-behavior

# Create analysis tables via SQL
dolt sql -q "CREATE TABLE user_metrics AS 
            SELECT user_id, COUNT(*) as actions 
            FROM user_actions 
            GROUP BY user_id;"

# Create tests to validate analysis
dolt sql -q "INSERT INTO `dolt_tests` VALUES ('test_metrics_created', 'analysis', 'SELECT COUNT(*) FROM user_metrics;', 'row_count', '>', '0');"
dolt sql -q "INSERT INTO `dolt_tests` VALUES ('test_metrics_integrity', 'integrity', 'SELECT COUNT(*) FROM user_metrics um LEFT JOIN users u ON um.user_id = u.id WHERE u.id IS NULL;', 'row_count', '==', '0');"

# Run tests to validate analysis
dolt sql -q "SELECT * FROM dolt_test_run();"

# Stage and commit using CLI
dolt add user_metrics
dolt commit -m "Add user behavior analysis"
```

## Integration with External Tools

### Database Clients
Most MySQL clients work with Dolt:
- MySQL Workbench
- phpMyAdmin  
- DataGrip
- DBeaver

### Backup and Sync
```bash
# Push to remote
dolt push origin main

# Pull changes
dolt pull origin main

# Clone for backup
dolt clone <remote-url> backup-location
```

This guide enables agents to leverage Dolt's unique version control capabilities while maintaining data integrity and following collaborative development practices.