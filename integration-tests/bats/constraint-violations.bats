#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "constraint-violations: functions blocked with violations" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt commit -Am "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3), (9, 9)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main

    run dolt merge other
    log_status_eq "1"
    [[ "$output" =~ "Fix constraint violations" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt status
    log_status_eq "0"
    [[ "$output" =~ "fix constraint violations" ]] || false
    [[ "$output" =~ "test" ]] || false
    run dolt merge other
    log_status_eq "1"
    [[ "$output" =~ "merging is not possible because you have not committed an active merge" ]] || false

    # we can stage conflicts, but not commit them
    dolt add test
    run dolt commit -m "this should fail"
    log_status_eq "1"
    [[ "$output" =~ "constraint violation" ]] || false

    dolt sql -q "DELETE FROM dolt_constraint_violations_test"
    run dolt status
    log_status_eq "0"
    [[ "$output" =~ "constraint violations fixed" ]] || false
    dolt add test
    dolt commit -m "this works"
    run dolt merge other
    log_status_eq "0"
}

@test "constraint-violations: dolt_force_transaction_commit along with dolt_allow_commit_conflicts ignores constraint violations" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3), (9, 9)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main

    run dolt sql <<"SQL"
SET dolt_allow_commit_conflicts = 0;
call dolt_merge('other');
SQL
    log_status_eq "1"
    [[ "$output" =~ "constraint violations" ]] || false
    run dolt sql <<"SQL"
SET dolt_force_transaction_commit = 1;
call dolt_merge('other');
call dolt_commit("-am", "msg", "--force");
SQL
    log_status_eq "0"
    [[ ! "$output" =~ "constraint violations" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove with backup, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other -m "merge other"

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove with backup, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other -m "merge other"

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing both, other illegal operations, restrict" {
    dolt sql <<"SQL"
CREATE TABLE unrelated (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO unrelated VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO unrelated VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (1, 1), (2, 2);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_name"", ""Table"": ""child"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent deleted on theirs, child added on ours" {
    # Only child1 has a constraint violation, as child2 properly references the merged in values of child1
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 200"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Index"": ""fk_c1"", ""Table"": ""child1"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_c1"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent deleted on ours, child added on theirs" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 200"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Index"": ""fk_c1"", ""Table"": ""child1"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_c1"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent updated on theirs, child added on ours" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "UPDATE parent SET v1 = 3 WHERE pk = 200"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Index"": ""fk_c1"", ""Table"": ""child1"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_c1"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "$output" =~ "200,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent updated on ours, child added on theirs" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "UPDATE parent SET v1 = 3 WHERE pk = 200"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Index"": ""fk_c1"", ""Table"": ""child1"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_c1"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "$output" =~ "200,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: cyclic foreign keys, illegal deletion" {

    # We're deleting a reference in a cycle from each table to make sure it properly applies a violation in both instances
    dolt sql <<"SQL"
CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE t2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_t2 FOREIGN KEY (v1) REFERENCES t1 (v1));
ALTER TABLE t1 ADD CONSTRAINT fk_t1 FOREIGN KEY (v1) REFERENCES t2 (v1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO t1 VALUES (10, 1), (20, 2);
INSERT INTO t2 VALUES (1, 1), (2, 2);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch main2
    dolt branch other
    dolt branch other2
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM t1 WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout other2
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM t2 WHERE pk = 2;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC2"
    dolt checkout main
    dolt merge other

    # FF merge no longer checks constraints; forced commits require constraint reification
    run dolt constraints verify --all
    log_status_eq "1"

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "t2,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t2" -r=csv
    log_status_eq "0"
    echo $output
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Index"": ""fk_t2"", ""Table"": ""t2"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_t2"", ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""t1"", ""ReferencedColumns"": [""v1""]' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM t1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM t2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    dolt reset --hard
    dolt checkout main2
    dolt merge other2

    # FF merge no longer checks constraints; forced commits require constraint reification
    run dolt constraints verify --all
    log_status_eq "1"

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "t1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t1" -r=csv
    log_status_eq "0"
    echo $output
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    echo "OUTPUT: $output"
    [[ "$output" =~ 'foreign key,20,2,"{""Index"": ""v1"", ""Table"": ""t1"", ""Columns"": [""v1""], ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ForeignKey"": ""fk_t1"", ""ReferencedIndex"": ""fk_t2"", ""ReferencedTable"": ""t2"", ""ReferencedColumns"": [""v1""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM t1" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t2" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "constraint-violations: self-referential foreign keys, add on ours, delete on theirs" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES test (pk) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO test VALUES (1, NULL), (2, 1), (3, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (4, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM test WHERE pk = 3"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,4,3,"{""Index"": ""fk_name"", ""Table"": ""test"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": """", ""ReferencedTable"": ""test"", ""ReferencedColumns"": [""pk""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1," ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: self-referential foreign keys, add on theirs, delete on ours" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES test (pk) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO test VALUES (1, NULL), (2, 1), (3, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM test WHERE pk = 3"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    run dolt merge other
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,4,3,"{""Index"": ""fk_name"", ""Table"": ""test"", ""Columns"": [""v1""], ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ForeignKey"": ""fk_name"", ""ReferencedIndex"": """", ""ReferencedTable"": ""test"", ""ReferencedColumns"": [""pk""]}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1," ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: unique key violations create unmerged tables" {
    dolt sql <<SQL
CREATE TABLE t (
  pk int PRIMARY KEY,
  col1 int
);
CALL DOLT_ADD('.');

CALL DOLT_COMMIT('-am', 'create table');
CALL DOLT_BRANCH('right');
ALTER TABLE t ADD UNIQUE uniq_col1 (col1);
CALL DOLT_COMMIT('-am', 'add dada');

CALL DOLT_CHECKOUT('right');
INSERT INTO t VALUES (1, 1), (2, 1);
CALL DOLT_COMMIT('-am', 'add unique key constraint');

CALL DOLT_CHECKOUT('main');
SQL
    run dolt merge right
    log_status_eq 1
    [[ $output =~ "CONSTRAINT VIOLATION (content): Merge created constraint violation in t" ]] || false
    [[ $output =~ "Automatic merge failed; 1 table(s) are unmerged." ]] || false
}

@test "constraint-violations: altering FKs over PKs does not create bad index" {
    dolt sql <<SQL
set foreign_key_checks=0;
create table child (j int primary key, foreign key (j) references parent (i));
create table parent (i int primary key);
set foreign_key_checks=1;
delete from parent where i = 0;
SQL

    run dolt index ls
    [[ "$output" =~ "No indexes in the working set" ]] || false
}

@test "constraint-violations: keyless table constraint violations" {
  dolt sql <<"SQL"
CREATE TABLE aTable (aColumn INT NULL, bColumn INT NULL, UNIQUE INDEX aColumn_UNIQUE (aColumn ASC) VISIBLE, UNIQUE INDEX bColumn_UNIQUE (bColumn ASC) VISIBLE);

CALL dolt_commit('-Am', 'add tables');
CALL dolt_checkout('-b', 'side');
INSERT INTO aTable VALUES (1,2);
CALL dolt_commit('-am', 'add side data');

CALL dolt_checkout('main');
INSERT INTO aTable VALUES (1,3);
CALL dolt_commit('-am', 'add main data');
SET @@dolt_force_transaction_commit=1;
CALL dolt_checkout('side');
CALL DOLT_CHERRY_PICK(hashof('main'));
SQL

    # check the contents of our table
    dolt checkout side
    run dolt sql -q "SELECT * FROM aTable" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "aColumn,bColumn" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "1,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    # check the contents of the dolt constraint violation tables
    run dolt sql -q "SELECT * from dolt_constraint_violations" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "aTable,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "SELECT * from dolt_status" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "aTable,false,constraint violation" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "SELECT from_root_ish,violation_type,hex(dolt_row_hash) as dolt_row_hash,aColumn,bColumn,violation_info from dolt_constraint_violations_aTable" -r=csv
    log_status_eq "0"
    [[ "$output" =~ "from_root_ish,violation_type,dolt_row_hash,aColumn,bColumn,violation_info" ]] || false
    [[ "$output" =~ ',unique index,5A1ED8633E1842FCA8EE529E4F1C5944,1,2,"{""Name"": ""aColumn_UNIQUE"", ""Columns"": [""aColumn""]}"' ]] || false
    [[ "$output" =~ ',unique index,A922BFBF4E5489501A3808BC5CD702C0,1,3,"{""Name"": ""aColumn_UNIQUE"", ""Columns"": [""aColumn""]}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    # Fix the violations and clear out the constraint violations artifacts
    dolt sql -q "SET @@dolt_force_transaction_commit=1; UPDATE aTable SET aColumn = 2 WHERE bColumn = 3;"
    dolt sql -q "DELETE FROM dolt_constraint_violations_aTable;"

    run dolt sql -q "SELECT count(*) as count from dolt_constraint_violations_aTable"
    log_status_eq "0"
    [[ "$output" =~ "| count |" ]] || false
    [[ "$output" =~ "| 0     |" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}
