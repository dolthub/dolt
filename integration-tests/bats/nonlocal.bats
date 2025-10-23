#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "nonlocal: basic case" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch", "main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  # Nonlocal tables appear in "show create", but the output matches the aliased table.
  run dolt sql -q "show create table table_alias_branch"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "aliased_table" ]] || false
}

@test "nonlocal: branch name reflects the working set of the referenced branch" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch", "main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "nonlocal: branch ref reflects the committed version of the parent" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ('amzmapqt');
  CALL DOLT_COMMIT('-Am', 'create table');
  INSERT INTO aliased_table VALUES ('eesekkgo');

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_branch_ref", "heads/main", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias_branch_ref;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
  ! [[ "$output" =~ "eesekkgo" ]] || false
}

@test "nonlocal: tag and hash" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");
  CALL DOLT_COMMIT('-Am', 'commit');
  CALL DOLT_TAG('v1.0');

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
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

  # These nonlocal tables are read-only because they reference a read-only ref
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

@test "nonlocal: remote ref" {
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
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
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

@test "nonlocal: default ref" {
  # If unspecified, the ref defaults to the current HEAD.
  # This allows one table to alias another table on the same branch.
  dolt sql <<SQL
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  INSERT INTO dolt_nonlocal_tables(table_name, ref_table, options) VALUES
    ("table_alias", "aliased_table", "immediate");
SQL

  run dolt sql -q "select * from table_alias;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "nonlocal: default table_name" {
  # If unspecified, the parent table name defaults to the same table name as the child
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE table_alias (pk char(8) PRIMARY KEY);
  INSERT INTO table_alias VALUES ("amzmapqt");

  CALL DOLT_CHECKOUT('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
    ("table_alias", "main", "immediate");
SQL

  run dolt sql -q "select * from table_alias;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false
}

@test "nonlocal: wildcard table_name" {
  # The wildcard syntax matches the wildcard syntax used in dolt_ignore
  dolt checkout -b other
  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
    ("nonlocal_*", "main", "immediate");

  CALL DOLT_CHECKOUT('main');

  CREATE TABLE nonlocal_table1 (pk char(8) PRIMARY KEY);
  CREATE TABLE nonlocal_table2 (pk char(8) PRIMARY KEY);
  CREATE TABLE not_nonlocal_table (pk char(8) PRIMARY KEY);
  INSERT INTO nonlocal_table1 VALUES ("amzmapqt");
  INSERT INTO nonlocal_table2 VALUES ("eesekkgo");
  INSERT INTO not_nonlocal_table VALUES ("pzdxwmbd");

SQL

  run dolt sql -q "select * from nonlocal_table1;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "amzmapqt" ]] || false

  run dolt sql -q "select * from nonlocal_table2;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "eesekkgo" ]] || false

  run dolt sql -q "select * from not_nonlocal_table;"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "table not found" ]] || false
}

@test "nonlocal: a transaction that tries to update multiple branches fails as expected" {
  run dolt sql <<SQL
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  CALL DOLT_CHECKOUT('-b', 'other');
  CREATE TABLE local_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
      ("nonlocal_table", "main", "aliased_table", "immediate");
  set autocommit = 0;
  INSERT INTO local_table VALUES ("amzmapqt");
  INSERT INTO nonlocal_table VALUES ("eesekkgo");
  COMMIT;
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Cannot commit changes on more than one branch / database" ]] || false
}

@test "nonlocal: test foreign keys" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
SQL

  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
      ("nonlocal_table", "main", "aliased_table", "immediate");
  INSERT INTO nonlocal_table VALUES ("eesekkgo");
  CREATE TABLE local_table (pk char(8) PRIMARY KEY, FOREIGN KEY (pk) REFERENCES nonlocal_table(pk));
SQL

  dolt sql -q 'INSERT INTO local_table VALUES ("eesekkgo");'

  run dolt sql -q 'INSERT INTO local_table VALUES ("fdnfjfjf");'
  [ "$status" -eq 1 ]
  [[ "$output" =~ 'Foreign key violation on fk: `local_table_ibfk_1`, table: `local_table`, referenced table: `nonlocal_table`, key: `[fdnfjfjf]`' ]] || false

  # The current foreign keys hold, so they should validate
  dolt constraints verify
  dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('--all');"

  # It's possible for foreign keys on nonlocal tables to become invalidated due to changes on the nonlocal
  # branch, but this can be detected with dolt constraints verify

  dolt sql -q  'CALL DOLT_CHECKOUT("main"); DELETE FROM aliased_table;'
  run dolt constraints verify
  [ "$status" -eq 1 ]
  [[ "$output" =~ 'dolt_constraint_violations_local_table' ]] || false
  [[ "$output" =~ '| foreign key    | eesekkgo | {"Index": "", "Table": "local_table", "Columns": ["pk"], "OnDelete": "RESTRICT", "OnUpdate": "RESTRICT", "ForeignKey": "local_table_ibfk_1", "ReferencedIndex": "", "ReferencedTable": "nonlocal_table", "ReferencedColumns": ["pk"]} |' ]] || false

  run dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('--all');"
  [ "$status" -eq 1 ]
  [[ "$output" =~ 'ForeignKey: local_table_ibfk_1' ]] || false

  # Check that neither command removed the FK relation (this can happen if it thinks the child table was dropped)
  run dolt sql -q 'SHOW CREATE TABLE local_table;'
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'local_table_ibfk_1' ]] || false

  # Now try deleting the parent table and confirm that verifies correctly too.
  dolt sql -q  'CALL DOLT_CHECKOUT("main"); DROP TABLE aliased_table;'

  # dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('--all')"
  run dolt constraints verify
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ 'table not found' ]] || false

  run dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('--all');"
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ 'ForeignKey: local_table_ibfk_1' ]] || false

  # Check that neither command removed the FK relation (this can happen if it thinks the child table was dropped)
  run dolt sql -q 'SHOW CREATE TABLE local_table;'
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'local_table_ibfk_1' ]] || false
}

@test "nonlocal: trying to dolt_add a nonlocal table returns the appropriate warning" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);
SQL

  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");
  INSERT INTO nonlocal_table values ('ghdsgerg');
SQL

  run dolt add nonlocal_table
  [ "$status" -eq 1 ]
  [[ "$output" =~ "the table(s) nonlocal_table do not exist" ]] || false
}

@test "nonlocal: dolt_add('.') doesn't add nonlocal tables" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL DOLT_CHECKOUT('main');
  CREATE TABLE test_table (pk char(8) PRIMARY KEY);
SQL

  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
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

@test "nonlocal: self-referrential nonlocal tables in the same branch as their target are effectively ignored" {
  dolt sql <<SQL
  CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");

  INSERT INTO nonlocal_table values ('ghdsgerg');
SQL

  dolt add nonlocal_table
  run dolt sql -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  echo "$output"
  [[ "$output" =~ "nonlocal_table       | true" ]] || false

  # Unstage nonlocal_table but keep it in the working set
  dolt reset HEAD

  dolt add .
  run dolt sql -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "nonlocal_table       | true" ]] || false
}

@test "nonlocal: invalid options detected" {
  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "invalid");
SQL

  run dolt sql -q "select * from nonlocal_table;"
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ "Invalid nonlocal table options" ]] || false
}

# The below tests are convenience features but not necessary for the MVP

@test "nonlocal: nonlocal tables appear in show_tables" {
  dolt checkout -b other
  dolt sql <<SQL
  CALL dolt_checkout('main');
  CREATE TABLE aliased_table (pk char(8) PRIMARY KEY);
  CREATE TABLE table_alias_1 (pk char(8) PRIMARY KEY);
  CREATE TABLE table_alias_wild_3 (pk char(8) PRIMARY KEY);
  INSERT INTO aliased_table VALUES ("amzmapqt");

  CALL dolt_checkout('other');
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
    ("table_alias_1", "main", "", "immediate"),
    ("table_alias_2", "main", "aliased_table", "immediate"),
    ("table_alias_wild_*", "main", "", "immediate"),
    ("table_alias_missing", "main", "", "immediate");
SQL

  # Nonlocal tables should appear in "show tables"
  run dolt sql -q "show tables"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "table_alias_1" ]] || false
  [[ "$output" =~ "table_alias_2" ]] || false
  [[ "$output" =~ "table_alias_wild_3" ]] || false
  ! [[ "$output" =~ "table_alias_missing" ]] || false
}

@test "nonlocal: creating a table matching a nonlocal table rule results in an error" {
  dolt checkout -b other
  dolt sql <<SQL
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");
SQL

  run dolt sql -q "CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Cannot create table name nonlocal_table because it matches a name present in dolt_nonlocal_tables." ]] || false
}

@test "nonlocal: adding an existing table to nonlocal tables errors" {
  skip
  dolt checkout -b other
  run dolt sql <<SQL
  CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");
SQL
  [ "$status" -eq 0 ]
  [[ "$output" =~ "cannot make nonlocal table nonlocal_table, table already exists on branch other" ]] || false
}