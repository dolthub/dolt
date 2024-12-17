#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-diff: output reconciles INSERT query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'INSERT INTO test (pk, c1, c2, c3, c4, c5) VALUES (2, 11, 0, 0, 0, 0)'
    dolt add test
    dolt commit -m "Added three rows"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    cat query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles UPDATE query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET c1=11, c5=6 WHERE pk=0'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles DELETE query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'DELETE FROM test WHERE pk=0'
    dolt add test
    dolt commit -m "deleted first row"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles change to PRIMARY KEY field in row" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pk5col-ints.csv`
    dolt add test
    dolt commit -m "Added one initial row"

    dolt checkout -b newbranch
    dolt sql -q 'UPDATE test SET pk=2 WHERE pk=1'
    dolt add test
    dolt commit -m "modified first row"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles column rename" {

    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test rename column c1 to c0"
    dolt add .
    dolt commit -m "renamed column"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    cat query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles DROP column query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test drop column c1"
    dolt add .
    dolt commit -m "dropped column"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch

    cat query
    dolt sql < query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: output reconciles ADD column query" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "added row"

    dolt checkout -b newbranch
    dolt sql -q "alter table test add c0 bigint"
    dolt add .
    dolt commit -m "added column"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: reconciles CREATE TABLE" {
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    echo "----------------------"
    cat query
    echo "----------------------"
    dolt checkout firstbranch
    dolt sql < query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: includes row INSERTSs to new tables after CREATE TABLE" {
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt sql -q 'insert into test values (2,2,2,2,2,2)'
    dolt sql -q 'insert into test values (3,3,3,3,3,3)'
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ] || false

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    cat query
    dolt sql < query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    dolt diff -r sql firstbranch newbranch
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: reconciles DROP TABLE" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "setup table"

    dolt checkout -b newbranch
    dolt sql -q 'drop table test'
    dolt add .
    dolt commit -m "removed table"

    # confirm a difference exists
    dolt diff -r sql firstbranch newbranch
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    cat query
    dolt sql < query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: reconciles RENAME TABLE" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "created table"

    dolt checkout -b newbranch
    dolt sql -q='RENAME TABLE test TO newname'
    dolt sql -q 'insert into newname values (2,1,1,1,1,1)'
    dolt add .
    dolt commit -m "renamed table and added data"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    cat query
    dolt sql < query
    dolt add .
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    grep 'RENAME' query
}

@test "sql-diff: reconciles RENAME TABLE with schema changes" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m "created table"

    dolt checkout -b newbranch
    dolt sql -q 'ALTER TABLE test RENAME COLUMN c2 to col2'
    dolt sql -q 'ALTER TABLE test ADD COLUMN c6 int'
    dolt sql -q='RENAME TABLE test TO newname'
    dolt sql -q 'ALTER TABLE newname DROP COLUMN c3'
    dolt sql -q 'insert into newname values (2,1,1,1,1,1)'
    dolt add .
    dolt commit -m "renamed table and added data"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    cat query
    dolt sql < query
    dolt add .
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    skip "this test is generating extra sql"
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    grep 'RENAME' query
}

@test "sql-diff: reconciles CREATE/ALTER/DROP VIEW" {
    dolt sql -q 'create table test (pk int not null primary key)'
    dolt sql -q 'create view double_view as select pk*2 from test'
    run dolt diff -r sql
    [ "$status" -eq 0 ]
    skip "create view statements not implemented"
    [[ "$output" =~ "CREATE VIEW `double_view`" ]] || false
}

@test "sql-diff: diff sql recreates tables with all types" {
    dolt checkout -b firstbranch
    dolt checkout -b newbranch
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    # dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "created new table"

    # confirm a difference exists
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ ! "$output" = "" ]

    dolt diff -r sql firstbranch newbranch > query
    dolt checkout firstbranch
    dolt sql < query
    rm query
    dolt add test
    dolt commit -m "Reconciled with newbranch"

    # confirm that both branches have the same content
    dolt diff -r sql firstbranch newbranch
    run dolt diff -r sql firstbranch newbranch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: supports all types" {
    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    dolt table import -u test `batshelper 1pksupportedtypes.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/1pksuppportedtypes/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update create_table
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/1pksupportedtypes/$query.sql
        dolt add .
        dolt commit -m "applied $query query"

        # confirm a difference exists
        run dolt diff -r sql firstbranch newbranch
        [ "$status" -eq 0 ]
        [ ! "$output" = "" ]

        dolt diff -r sql firstbranch > patch.sql newbranch
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add .
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff -r sql firstbranch newbranch
        [ "$status" -eq 0 ]
        [ "$output" = "" ]
    done
}

@test "sql-diff: supports multiple primary keys" {
    dolt sql -q "set @@PERSIST.dolt_stats_auto_refresh_enabled = 0;"
    dolt sql -q "set @@PERSIST.dolt_stats_bootstrap_enabled = 0;"

    dolt checkout -b firstbranch
    dolt sql <<SQL
CREATE TABLE test (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt table import -u test `batshelper 2pk5col-ints.csv`
    dolt add .
    dolt commit -m "create/init table test"

    # for each query file in helper/queries/2pk5col-ints/
    # run query on db, create sql diff patch, confirm they're equivalent
    dolt branch newbranch
    for query in delete add update single_pk_update all_pk_update create_table
    do
        dolt checkout newbranch
        dolt sql < $BATS_TEST_DIRNAME/helper/queries/2pk5col-ints/$query.sql
        dolt add .
        dolt diff -r sql
        dolt commit -m "applied $query query "

        # confirm a difference exists

        run dolt diff -r sql firstbranch newbranch
        [ "$status" -eq 0 ]
        [ ! "$output" = "" ]

        dolt diff -r sql firstbranch > patch.sql newbranch
        dolt checkout firstbranch
        dolt sql < patch.sql
        rm patch.sql
        dolt add .
        dolt commit -m "Reconciled with newbranch"

        # confirm that both branches have the same content
        run dolt diff -r sql firstbranch newbranch
        [ "$status" -eq 0 ]
        [ "$output" = "" ]
    done
}

@test "sql-diff: escapes values for MySQL string literals" {
    # https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 TEXT COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt add .
    dolt commit -m "created table"
    dolt branch other

    dolt sql -q "insert into test (pk, c1) values (0, '\\\\')";
    dolt sql -q "insert into test (pk, c1) values (1, 'this string ends in backslash\\\\')";
    dolt sql -q "insert into test (pk, c1) values (2, 'this string has \\\"double quotes\\\" in it')";
    dolt sql -q "insert into test (pk, c1) values (3, 'it\\'s a contraction y\\'all')";
    dolt sql -q "insert into test (pk, c1) values (4, 'backspace \\\b')";
    dolt sql -q "insert into test (pk, c1) values (5, 'newline \\\n')";
    dolt sql -q "insert into test (pk, c1) values (6, 'carriage return \\\r')";
    dolt sql -q "insert into test (pk, c1) values (7, 'tab \\\t')";
    dolt sql -q "insert into test (pk, c1) values (8, 'ASCII 26 (Control+Z) \\Z')";
    dolt sql -q "insert into test (pk, c1) values (9, 'percent \\%')";
    dolt sql -q "insert into test (pk, c1) values (10,'underscore \\_')";
    dolt sql -q "insert into test (pk, c1) values (11,'\\\"\\\"')";
    dolt sql -q "insert into test (pk, c1) values (12,'\\\"')";

    dolt add .
    dolt commit -m "added tricky rows"
    dolt checkout other
    dolt diff -r sql other main > patch.sql
    run dolt sql < patch.sql
    [ "$status" -eq 0 ]
    run dolt diff -r sql main
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "sql-diff: sql diff ignores dolt docs" {
    echo "This is a README" > README.md
    run dolt diff -r sql
    [ "$status" -eq 0 ]
    skip "Have to decide how to treat dolty_docs in diff -r sql"
    [[ ! "$output" =~ "dolt_docs" ]] || false;
}

@test "sql-diff: handles NULL cells" {
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 TEXT COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt sql -q "insert into test (pk, c1) values (0, NULL)"
    dolt diff -r sql
    run dolt diff -r sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`c1`) VALUES (0,NULL)' ]] || false

    dolt sql -q "drop table test"
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 DATETIME COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt sql -q "insert into test (pk, c1) values (0, NULL)"
    run dolt diff -r sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`c1`) VALUES (0,NULL)' ]] || false
}

@test "sql-diff: views, triggers, tables" {
    dolt sql <<SQL
create table test (pk int primary key, c1 int, c2 int);
call dolt_add('.');
insert into test values (1,2,3);
insert into test values (4,5,6);
SQL
    dolt commit -am "Table with rows"

    dolt sql <<SQL
insert into test values (7,8,9);
delete from test where pk = 4;
SQL
    dolt commit -Am "Table data diff"

    dolt sql <<SQL
create view v1 as select "hello" from test;
SQL
    dolt commit -Am "View"

    dolt sql <<SQL
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 1;
SQL
    dolt commit -Am "Trigger"

    # Only table data diff
    dolt diff -r sql HEAD~3 HEAD~2
    run dolt diff -r sql HEAD~3 HEAD~2
    [ "$status" -eq 0 ]

    cat > expected <<'EOF'
DELETE FROM `test` WHERE `pk`=4;
INSERT INTO `test` (`pk`,`c1`,`c2`) VALUES (7,8,9);
EOF

    # We can't do direct bash comparisons because of newlines, so use diff to compare
    # Diff returns non-zero unless empty
    cat > actual <<EOF
${output}
EOF
    diff -w expected actual

    # Only view diff
    dolt diff -r sql HEAD~2 HEAD~
    run dolt diff -r sql HEAD~2 HEAD~
    [ "$status" -eq 0 ]

    cat > expected <<'EOF'
create view v1 as select "hello" from test;
EOF

    cat > actual <<EOF
${output}
EOF

    diff -w expected actual

    # Only trigger diff
    dolt diff -r sql HEAD~ HEAD
    run dolt diff -r sql HEAD~ HEAD
    [ "$status" -eq 0 ]
    
    cat > expected <<'EOF'
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 1;
EOF

    cat > actual <<EOF
${output}
EOF
    
    diff -w expected actual

    # View and trigger diff
    dolt diff -r sql HEAD~2 HEAD
    run dolt diff -r sql HEAD~2 HEAD
    [ "$status" -eq 0 ]

    cat > expected <<EOF
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 1;
create view v1 as select "hello" from test;
EOF
    
    cat > actual <<EOF
${output}
EOF

    diff -w expected actual

    # Table and view diff
    dolt diff -r sql HEAD~3 HEAD~
    run dolt diff -r sql HEAD~3 HEAD~
    [ "$status" -eq 0 ]

    cat > expected <<'EOF'
DELETE FROM `test` WHERE `pk`=4;
INSERT INTO `test` (`pk`,`c1`,`c2`) VALUES (7,8,9);
create view v1 as select "hello" from test;
EOF

    cat > actual <<EOF
${output}
EOF

    diff -w expected actual

    # All three kinds of diff
    dolt diff -r sql HEAD~3 HEAD
    run dolt diff -r sql HEAD~3 HEAD
    [ "$status" -eq 0 ]

    cat > expected <<'EOF'
DELETE FROM `test` WHERE `pk`=4;
INSERT INTO `test` (`pk`,`c1`,`c2`) VALUES (7,8,9);
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 1;
create view v1 as select "hello" from test;
EOF

    cat > actual <<EOF
${output}
EOF

    diff -w expected actual

    # check alterations of triggers and views
    dolt sql <<SQL
drop trigger tr1;
drop view v1;
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 100;
create view v1 as select "goodbye" from test;
SQL

    dolt commit -am "new view and trigger defs"

    dolt diff -r sql HEAD~ HEAD
    run dolt diff -r sql HEAD~ HEAD
    [ "$status" -eq 0 ]

    cat > expected <<'EOF'
DROP TRIGGER `tr1`;
create trigger tr1 before insert on test for each row set new.c1 = new.c1 + 100;
DROP VIEW `v1`;
create view v1 as select "goodbye" from test;
EOF

    cat > actual <<EOF
${output}
EOF

    diff -w expected actual    
}

@test "sql-diff: stat" {
    dolt sql -q "create table t (i int primary key);"
    run dolt diff --stat -r sql
    [ "$status" -eq 1 ]
    [[ "$output" =~ "diff stats are not supported for sql output" ]] || false
}
