@test "sql diff escapes values that end in backslash correctly" {
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 TEXT COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt sql -q "insert into test (pk, c1) values (0, '\\\\')";
    dolt sql -q	"insert into test (pk, c1) values (1, 'this string ends in backslash\\\\')";
    dolt diff --sql > $BATS_TMPDIR/input-$$.sql
    run dolt sql < $BATS_TMPDIR/input-$$.sql
    skip "backslashes at the end of strings not supported correctly by sql diff"
    [ "$status" -eq 0 ]
}

@test "sql diff ignores dolt docs" {
    echo "This is a README" > README.md 
    run dolt diff --sql
    [ "$status" -eq 0 ]
    skip "Have to decide how to treat dolty_docs in diff --sql"
    [[ ! "$output" =~ "dolt_docs" ]] || false;
}

@test "sql diff handles NULL cells" {
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 TEXT COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt sql -q "insert into test (pk, c1) values (0, NULL)";
    run dolt diff --sql
    [ "$status" -eq 0 ]
    skip "dolt diff -sql prints out NULL values right now"
    [[ ! "$output" =~ "NULL" ]] || false;
    dolt sql -q "drop table test"
    dolt sql <<SQL
CREATE TABLE test (
  pk INT NOT NULL COMMENT 'tag:0',
  c1 DATETIME COMMENT 'tag:1',
  PRIMARY KEY(pk)
);
SQL
    dolt sql -q "insert into test (pk, c1) values (0, NULL)";
    run dolt diff --sql
    skip "dolt diff -sql fails with filed to tranform row pk:0 |"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "failed to transform" ]] || false 
}
