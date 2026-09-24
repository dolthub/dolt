#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

setup_nonlocal_transaction_tables() {
  dolt sql <<SQL
  CREATE TABLE aliased_table (pk INT PRIMARY KEY, v INT);
  INSERT INTO aliased_table VALUES (1, 10), (2, 20);
  CALL DOLT_COMMIT('-Am', 'set up main');
  CALL DOLT_CHECKOUT('-b', 'other');
  CREATE TABLE local_table (pk INT PRIMARY KEY, v INT);
  INSERT INTO local_table VALUES (1, 100), (2, 200);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, ref_table, options) VALUES
      ('nonlocal_table', 'main', 'aliased_table', 'immediate');
  CALL DOLT_COMMIT('-Am', 'set up other');
SQL
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

@test "nonlocal: a transaction commits inserts to local and nonlocal branches" {
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
  [ "$status" -eq 0 ]

  # Read in new sessions to verify both working sets were persisted.
  run dolt --branch other sql -r csv -q 'SELECT * FROM local_table;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk\namzmapqt' ]

  run dolt --branch other sql -r csv -q 'SELECT * FROM nonlocal_table;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk\neesekkgo' ]

  run dolt --branch main sql -r csv -q 'SELECT * FROM aliased_table;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk\neesekkgo' ]
}

@test "nonlocal: a transaction commits updates and deletes to local and nonlocal branches" {
  setup_nonlocal_transaction_tables

  run dolt --branch other sql <<SQL
  START TRANSACTION;
  UPDATE local_table SET v = v + 1 WHERE pk = 1;
  UPDATE nonlocal_table SET v = v + 5 WHERE pk = 1;
  DELETE FROM local_table WHERE pk = 2;
  DELETE FROM nonlocal_table WHERE pk = 2;
  COMMIT;
SQL
  [ "$status" -eq 0 ]

  run dolt --branch other sql -r csv -q 'SELECT * FROM local_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,101' ]

  run dolt --branch other sql -r csv -q 'SELECT * FROM nonlocal_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,15' ]

  run dolt --branch main sql -r csv -q 'SELECT * FROM aliased_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,15' ]

  # The alias must not update the same-named table on the local branch.
  run dolt --branch other sql -r csv -q 'SELECT * FROM aliased_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,10\n2,20' ]
}

@test "nonlocal: savepoint rollback preserves earlier changes on both branches" {
  setup_nonlocal_transaction_tables

  run dolt --branch other sql <<SQL
  START TRANSACTION;
  UPDATE local_table SET v = 101 WHERE pk = 1;
  UPDATE nonlocal_table SET v = 11 WHERE pk = 1;
  SAVEPOINT both_branches;
  DELETE FROM local_table WHERE pk = 1;
  DELETE FROM nonlocal_table WHERE pk = 1;
  INSERT INTO local_table VALUES (3, 300);
  INSERT INTO nonlocal_table VALUES (3, 30);
  ROLLBACK TO SAVEPOINT both_branches;
  COMMIT;
SQL
  [ "$status" -eq 0 ]

  run dolt --branch other sql -r csv -q 'SELECT * FROM local_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,101\n2,200' ]

  run dolt --branch main sql -r csv -q 'SELECT * FROM aliased_table ORDER BY pk;'
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,11\n2,20' ]
}

@test "nonlocal: dolt_commit_all commits local and nonlocal branch heads" {
  setup_nonlocal_transaction_tables

  run dolt --branch other sql <<SQL
  START TRANSACTION;
  UPDATE local_table SET v = 101 WHERE pk = 1;
  UPDATE nonlocal_table SET v = 11 WHERE pk = 1;
  CALL DOLT_COMMIT_ALL('-am', 'update both branches');
SQL
  [ "$status" -eq 0 ]

  # Verify the branch HEADs, not just their working sets.
  run dolt --branch other sql -r csv -q "SELECT * FROM local_table AS OF 'HEAD' ORDER BY pk;"
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,101\n2,200' ]

  run dolt --branch main sql -r csv -q "SELECT * FROM aliased_table AS OF 'HEAD' ORDER BY pk;"
  [ "$status" -eq 0 ]
  [ "$output" = $'pk,v\n1,11\n2,20' ]

  for branch in main other; do
    run dolt --branch "$branch" sql -r csv -q 'SELECT message FROM dolt_log LIMIT 1;'
    [ "$status" -eq 0 ]
    [ "$output" = $'message\nupdate both branches' ]

    run dolt --branch "$branch" sql -r csv -q 'SELECT COUNT(*) AS dirty_tables FROM dolt_status;'
    [ "$status" -eq 0 ]
    [ "$output" = $'dirty_tables\n0' ]
  done
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

  # Check that verifying didn't remove the FK relation (this can happen if it thinks the child table was dropped)
  run dolt sql -q 'SHOW CREATE TABLE local_table;'
  [ "$status" -eq 0 ]
  [[ "$output" =~ 'local_table_ibfk_1' ]] || false
}

@test "nonlocal: detect foreign key violations when rows are deleted from the nonlocal table" {
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

  # It's possible for foreign keys on nonlocal tables to become invalidated due to changes on the nonlocal
  # branch, but this can be detected with dolt constraints verify

  dolt sql -q  'CALL DOLT_CHECKOUT("main"); DROP TABLE aliased_table;'

  # dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('--all')"
  run dolt constraints verify
  [ "$status" -eq 1 ]
  echo "$output"
  [[ "$output" =~ 'table not found' ]] || false

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

  run dolt sql -r csv -q "CALL DOLT_CHECKOUT('main'); SELECT * FROM dolt_status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "test_table,0" ]] || false
}

@test "nonlocal: self-referrential nonlocal tables in the same branch as their target are effectively ignored" {
  dolt sql <<SQL
  CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");

  INSERT INTO nonlocal_table values ('ghdsgerg');
SQL

  dolt add nonlocal_table
  run dolt sql -r csv -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  echo "$output"
  [[ "$output" =~ "nonlocal_table,1" ]] || false

  # Unstage nonlocal_table but keep it in the working set
  dolt reset HEAD

  dolt add .
  run dolt sql -r csv -q "select * from dolt_status"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "nonlocal_table,1" ]] || false
}

@test "nonlocal: adding an existing table to nonlocal tables errors" {
  skip
  dolt checkout -b other
  run dolt sql <<SQL
  CREATE TABLE nonlocal_table (pk char(8) PRIMARY KEY);
  INSERT INTO dolt_nonlocal_tables(table_name, target_ref, options) VALUES
      ("nonlocal_table", "main", "immediate");
SQL
  [ "$status" -eq 1 ]
  [[ "$output" =~ "cannot make nonlocal table nonlocal_table, table already exists on branch other" ]] || false
}
