#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
	setup_common
	dolt sql <<SQL
CREATE TABLE one_pk (
  pk1 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1)
);
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE TABLE empty (
  pk BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
  dolt add .
  dolt commit -m "create tables"
}

teardown() {
	teardown_common
}

@test "sql-conflicts: read from empty table" {
    dolt sql -q "SELECT * FROM dolt_conflicts_empty"
}

@test "sql-conflicts: add conflict" {
  dolt branch feature_branch main
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0)"
  dolt SQL -q "INSERT INTO two_pk (pk1,pk2,c1,c2) VALUES (0,0,0,0)"
  dolt add .
  dolt commit -m "changed main"
  dolt checkout feature_branch
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (0,1,1)"
  dolt SQL -q "INSERT INTO two_pk (pk1,pk2,c1,c2) VALUES (0,0,1,1)"
  dolt add .
  dolt commit -m "changed feature_branch"
  dolt checkout main
  dolt merge feature_branch

  EXPECTED=$( echo -e "table,num_conflicts\none_pk,1\ntwo_pk,1")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts ORDER BY \`table\`"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n,0,0,,0,1,,0,1")
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_one_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n,0,0,,0,0,,0,1,,0,1")
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_two_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  run dolt conflicts cat one_pk
  [ "$status" -eq 0 ]
  [[ ! "$output" =~ "base" ]]
  [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false

  run dolt conflicts cat two_pk
  [ "$status" -eq 0 ]
  [[ ! "$output" =~ "base" ]]
  [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false

  dolt sql  <<SQL
  set autocommit = off;
  DELETE from dolt_conflicts_two_pk WHERE our_pk1 = 0 and our_pk2 = 0;
  DELETE from dolt_conflicts_one_pk WHERE our_pk1 = 0;
  commit;
SQL

  EXPECTED=$( echo -e "table,num_conflicts")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  # delete an already resolved conflict a 2nd time is fine
  run dolt sql -q "DELETE from dolt_conflicts_one_pk WHERE our_pk1 = 0"
  [ "$status" -eq 0 ]
}


@test "sql-conflicts: modify conflict" {
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0)"
  dolt SQL -q "INSERT INTO two_pk (pk1,pk2,c1,c2) VALUES (0,0,0,0)"
  dolt add .
  dolt commit -m "initial values"
  dolt branch feature_branch main
  dolt SQL -q "UPDATE one_pk SET c1=1,c2=1 WHERE pk1=0"
  dolt SQL -q "UPDATE two_pk SET c1=1,c2=1 WHERE pk1=0 and PK2=0"
  dolt add .
  dolt commit -m "changed main"
  dolt checkout feature_branch
  dolt SQL -q "UPDATE one_pk SET c1=2,c2=2 WHERE pk1=0"
  dolt SQL -q "UPDATE two_pk SET c1=2,c2=2 WHERE pk1=0 and PK2=0"
  dolt add .
  dolt commit -m "changed feature_branch"
  dolt checkout main
  dolt merge feature_branch

  EXPECTED=$( echo -e "table,num_conflicts\none_pk,1\ntwo_pk,1")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts ORDER BY \`table\`"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n0,0,0,0,1,2,0,1,2" )
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_one_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n0,0,0,0,0,0,0,1,2,0,1,2" )
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_two_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  run dolt conflicts cat one_pk
  [ "$status" -eq 0 ]
  [[ "$output" =~ "base" ]]
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+theirs[[:space:]] ]] || false

  run dolt conflicts cat two_pk
  [ "$status" -eq 0 ]
  [[ "$output" =~ "base" ]]
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+theirs[[:space:]] ]] || false

  dolt sql  << SQL
set autocommit = off;
DELETE from dolt_conflicts_one_pk WHERE our_pk1 = 0;
DELETE from dolt_conflicts_two_pk WHERE our_pk1 = 0 and our_pk2 = 0;
commit;
SQL

  EXPECTED=$( echo -e "table,num_conflicts")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-conflicts: delete modify conflict" {
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0)"
  dolt SQL -q "INSERT INTO two_pk (pk1,pk2,c1,c2) VALUES (0,0,0,0)"
  dolt add .
  dolt commit -m "initial values"
  dolt branch feature_branch main
  dolt SQL -q "UPDATE one_pk SET c1=1,c2=1 WHERE pk1=0"
  dolt SQL -q "UPDATE two_pk SET c1=1,c2=1 WHERE pk1=0 and PK2=0"
  dolt add .
  dolt commit -m "changed main"
  dolt checkout feature_branch
  dolt SQL -q "DELETE FROM one_pk WHERE pk1=0"
  dolt SQL -q "DELETE FROM two_pk WHERE pk1=0 and PK2=0"
  dolt add .
  dolt commit -m "changed feature_branch"
  dolt checkout main
  dolt merge feature_branch

  EXPECTED=$( echo -e "table,num_conflicts\none_pk,1\ntwo_pk,1")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts ORDER BY \`table\`"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n0,0,,0,1,,0,1," )
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_one_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2\n0,0,,0,0,,0,1,,0,1," )
  run dolt sql -r csv -q "SELECT base_pk1,our_pk1,their_pk1,base_pk2,our_pk2,their_pk2,base_c1,our_c1,their_c1,base_c2,our_c2,their_c2 FROM dolt_conflicts_two_pk"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false

  run dolt conflicts cat one_pk
  [ "$status" -eq 0 ]
  [[ "$output" =~ "base" ]]
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \-[[:space:]]*\|[[:space:]]+theirs[[:space:]] ]] || false

  run dolt conflicts cat two_pk
  [ "$status" -eq 0 ]
  [[ "$output" =~ "base" ]]
  [[ "$output" =~ \*[[:space:]]*\|[[:space:]]+ours[[:space:]] ]] || false
  [[ "$output" =~ \-[[:space:]]*\|[[:space:]]+theirs[[:space:]] ]] || false

  dolt sql << SQL
set autocommit = off;
DELETE from dolt_conflicts_one_pk WHERE our_pk1 = 0;
DELETE from dolt_conflicts_two_pk WHERE our_pk1 = 0 and our_pk2 = 0;
commit;
SQL

  EXPECTED=$( echo -e "table,num_conflicts")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-conflicts: multiple conflicts" {
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (0,0,0)"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (1,0,0)"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (2,0,0)"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (3,0,0)"
  dolt add .
  dolt commit -m "initial values"
  dolt branch feature_branch main
  dolt SQL -q "UPDATE one_pk SET c1=1,c2=1"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (4,1,1)"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (5,3,3)"
  dolt add .
  dolt commit -m "changed main"
  dolt checkout feature_branch
  dolt SQL -q "UPDATE one_pk SET c1=2,c2=2"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (4,2,2)"
  dolt SQL -q "DELETE FROM one_pk WHERE pk1=3"
  dolt SQL -q "INSERT INTO one_pk (pk1,c1,c2) VALUES (5,3,3)"
  dolt add .
  dolt commit -m "changed feature_branch"
  dolt checkout main
  dolt merge feature_branch

  EXPECTED=$( echo -e "table,num_conflicts\none_pk,5")
  run dolt sql -r csv -q "SELECT * FROM dolt_conflicts"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false
}
