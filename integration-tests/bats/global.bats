#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "global: basic case" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch", "main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  # Global tables appear in "show create", but the output matches the aliased table.
  run dolt sql -q "show create table table_alias_branch"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "aliased_table" ]] || false
}

@test "global: branch name reflects the working set of the referenced branch" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch", "main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "global: branch ref reflects the committed version of the parent" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ('amzmapqt');
  CALL DOLT_COMMIT('-Am', 'create table');
  INSERT INTO aliased_table VALUES ('eesekkgo');

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch_ref", "heads/main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch_ref;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
  ! [[ "$output" =~ "eesekkgo" ]] || false
}

@test "global: tag and hash" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");
  CALL DOLT_COMMIT('-Am', 'commit');
  CALL DOLT_TAG('v1.0');

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_tag", "v1.0", "aliased_table", "immediate"),
    ("table_alias_tag_ref", "tags/v1.0", "aliased_table", "immediate"),
    ("table_alias_hash", DOLT_HASHOF('v1.0'), "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_tag_ref;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  run dolt sql -q "select * from table_alias_tag;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  run dolt sql -q "select * from table_alias_hash;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  # These global tables are read-only because they reference a read-only ref
  run dolt sql <<SQL
    INSERT INTO table_alias_tag_ref VALUES ("eesekkgo");
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "table doesn't support INSERT INTO" ]] || false

  run dolt sql <<SQL
    INSERT INTO table_alias_tag VALUES ("eesekkgo");
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "table doesn't support INSERT INTO" ]] || false

  run dolt sql <<SQL
    INSERT INTO table_alias_hash VALUES ("eesekkgo");
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "table doesn't support INSERT INTO" ]] || false
}

@test "global: remote ref" {
  mkdir child
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");
  CALL DOLT_COMMIT('-Am', 'create table');
  CALL DOLT_REMOTE('add', 'remote_db', 'file://./remote');
  CALL DOLT_PUSH('remote_db', 'main');

  -- drop table so it is only accessible from the remote ref
  DROP TABLE aliased_table;
  CALL DOLT_COMMIT('-am', 'drop table');

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_remote_branch", "remote_db/main", "aliased_table", "immediate"),
    ("table_alias_remote_ref", "remotes/remote_db/main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_remote_branch;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  run dolt sql -q "select * from table_alias_remote_ref;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "global: default ref" {
  # If unspecified, the ref defaults to the current HEAD.
  # This allows one table to alias another table on the same branch.
  dolt sql <<SQL
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  INSERT INTO dolt_global_tables(table_name, ref_table, options) VALUES
    ("table_alias", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "global: default table_name" {
  # If unspecified, the parent table name defaults to the same table name as the child
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE table_alias (pk char(8) PRIMARY KEY);
  INSERT INTO table_alias VALUES ("amzmapqt");

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
    ("table_alias", "main", "immediate");
SQL

  run dolt sql -q "select * from table_alias;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "global: wildcard table_name" {
  # The wildcard syntax matches the wildcard syntax used in dolt_ignore
  dolt checkout -b other
  dolt sql <<SQL
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
    ("global_*", "main", "immediate");

  CALL DOLT_CHECKOUT('main');

  CREATE TABLE global_table1 (pk char(8) PRIMARY KEY);
  CREATE TABLE global_table2 (pk char(8) PRIMARY KEY);
  CREATE TABLE not_global_table (pk char(8) PRIMARY KEY);
  INSERT INTO global_table1 VALUES ("amzmapqt");
  INSERT INTO global_table2 VALUES ("eesekkgo");
  INSERT INTO not_global_table VALUES ("pzdxwmbd");

SQL

  run dolt sql -q "select * from global_table1;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  run dolt sql -q "select * from global_table2;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "eesekkgo" ]] || false

  run dolt sql -q "select * from not_global_table;"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "table not found" ]] || false
}

@test "global: creating a global table creates it on the appropriate branch" {
  dolt checkout -b other
  dolt sql <<SQL
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("global_table", "main", "immediate");

  CREATE TABLE global_table (pk char(8) PRIMARY KEY);
  INSERT INTO global_table VALUES ("amzmapqt");
SQL

  run dolt ls main
  [ "$status" -eq 0 ]
  [[ "$output" =~ "global_table" ]] || false
}

@test "global: a transaction that tries to update multiple branches fails as expected" {
  run dolt sql <<SQL
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  CALL DOLT_CHECKOUT('-b', 'other');
  CREATE TABLE local_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
      ("global_table", "main", "aliased_table", "immediate");
  set autocommit = 0;
  INSERT INTO local_table VALUES ("amzmapqt");
  INSERT INTO global_table VALUES ("eesekkgo");
  COMMIT;
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Cannot commit changes on more than one branch / database" ]] || false
}

@test "global: test foreign keys" {
  # Currently, foreign keys cannot be added to global tables
  dolt checkout -b other
  run dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
      ("global_table", "main", "aliased_table", "immediate");
  set autocommit = 0;
  INSERT INTO global_table VALUES ("eesekkgo");

SQL

  run dolt sql <<SQL
  CREATE TABLE local_table (pk char(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES global_table(pk));
SQL

  [ "$status" -eq 1 ]
  [[ "$output" =~ "Cannot commit changes on more than one branch / database" ]] || false
}

@test "global: adding an existing table to global tables errors" {
  dolt checkout -b other
  run dolt sql <<SQL
  CREATE TABLE global_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("global_table", "main", "immediate");
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "cannot make global table global_table, table already exists on branch other" ]] || false
}

@test "global: global tables appear in show_tables" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_global_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch", "main", "aliased_table", "immediate");
SQL

  # Global tables should appear in "show tables"
  run dolt sql -q "show tables"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "table_alias_branch" ]] || false
}

@test "global: trying to dolt_add a global table returns the appropriate warning" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE global_table (pk char(8) PRIMARY KEY);
SQL

  dolt sql <<SQL
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("global_table", "main", "immediate");
  INSERT INTO global_table values ('ghdsgerg');

  CALL DOLT_ADD('global_table');
SQL

  run dolt status
  [ "$status" -eq 0 ]
  ! [[ "$output" =~ "global_table" ]] || false

  run dolt sql -q "CALL DOLT_CHECKOUT('main'); SELECT * FROM dolt_status"
  [ "$status" -eq 0 ]
  ! [[ "$output" =~ "global_table" ]] || false
  exit 1
}

@test "global: dolt_add('.') doesn't add global tables" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE test_table (pk char(8) PRIMARY KEY);
SQL

  dolt sql <<SQL
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("test_table", "main", "immediate");
  INSERT INTO test_table values ('ghdsgerg');

  CALL DOLT_ADD('.');
SQL

  run dolt sql -q "SELECT * FROM dolt_status"
  [ "$status" -eq 0 ]
  echo "$output"
  ! [[ "$output" =~ "test_table" ]] || false

  run dolt sql -q "CALL DOLT_CHECKOUT('main'); SELECT * FROM dolt_status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "test_table | false" ]] || false
}

@test "global: self-referrential global tables in the same branch as their target are effectively ignored" {
  dolt sql <<SQL
  CREATE TABLE global_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("global_table", "main", "immediate");

  INSERT INTO global_table values ('ghdsgerg');
SQL

  dolt add global_table
  run dolt sql -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  echo "$output"
  [[ "$output" =~ "global_table       | true" ]] || false

  # Unstage global_table but keep it in the working set
  dolt reset HEAD

  dolt add .
  run dolt sql -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "global_table       | true" ]] || false
}

@test "global: invalid options detected" {
  dolt sql <<SQL
  INSERT INTO dolt_global_tables(table_name, target_ref, options) VALUES
      ("global_table", "main", "invalid");
SQL

  run sql -q "select * from global_table;"
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ "Invalid global table options" ]] || false
}