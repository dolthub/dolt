#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    teardown_common
    cd ..
    rm -rf bats_repo
}

@test "dolt version" {
    run dolt version
    [ "$status" -eq 0 ]
    regex='dolt version [0-9]+.[0-9]+.[0-9]+'
    [[ "$output" =~ $regex ]] || false
}

@test "dolt status" {
    expected="On branch $DEFAULT_BRANCH"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$expected" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "dolt ls" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "Tables in working set:" ]] || false
}

@test "dolt branch" {
    run dolt branch
    [ "$status" -eq 0 ]
}

@test "dolt diff" {
    run dolt diff
    [ "$status" -eq 0 ]
}

@test "dolt schema show on branch init" {
    dolt checkout init
    run dolt schema show abc
    [ "$status" -eq 0 ]
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`w\` bigint" ]] || false
    [[ "${output}" =~ "\`x\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}

@test "dolt sql 'select * from abc' on branch init" {
    dolt checkout init
    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]


    [[ "${lines[1]}" =~ "| pk | a    | b   | w | x |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+---+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | 0 |" ]] || false
    [[ "${lines[4]}" =~ "| 1  | asdf | 1.1 | 0 | 0 |" ]] || false
    [[ "${lines[5]}" =~ "| 2  | asdf | 1.1 | 0 | 0 |" ]] || false
}

@test "dolt schema show on branch $DEFAULT_BRANCH" {
    run dolt schema show abc
    [ "$status" -eq 0 ]
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`x\` bigint" ]] || false
    [[ "${output}" =~ "\`y\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}


@test "dolt sql 'select * from abc' on branch $DEFAULT_BRANCH" {
    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| pk | a    | b   | x | y   |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+-----+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | 121 |" ]] || false
    [[ "${lines[4]}" =~ "| 2  | asdf | 1.1 | 0 | 121 |" ]] || false
    [[ "${lines[5]}" =~ "| 3  | data | 1.1 | 0 | 121 |" ]] || false
}

@test "dolt schema show on branch other" {
    dolt checkout other
    run dolt schema show abc
    [ "$status" -eq 0 ]
    echo $output
    output=`echo $output | tr '[:upper:]' '[:lower:]'` # lowercase the output
    [[ "${output}" =~ "abc @ working" ]] || false
    [[ "${output}" =~ "create table \`abc\` (" ]] || false
    [[ "${output}" =~ "\`pk\` bigint not null" ]] || false
    [[ "${output}" =~ "\`a\` longtext" ]] || false
    [[ "${output}" =~ "\`b\` double" ]] || false
    [[ "${output}" =~ "\`w\` bigint" ]] || false
    [[ "${output}" =~ "\`z\` bigint" ]] || false
    [[ "${output}" =~ "primary key (\`pk\`)" ]] || false
}

@test "dolt sql 'select * from abc' on branch other" {
    dolt checkout other
    run dolt sql -q 'select * from abc;'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| pk | a    | b   | w | z   |" ]] || false
    [[ "${lines[2]}" =~ "+----+------+-----+---+-----+" ]] || false
    [[ "${lines[3]}" =~ "| 0  | asdf | 1.1 | 0 | 122 |" ]] || false
    [[ "${lines[4]}" =~ "| 1  | asdf | 1.1 | 0 | 122 |" ]] || false
    [[ "${lines[5]}" =~ "| 4  | data | 1.1 | 0 | 122 |" ]] || false

    dolt checkout "$DEFAULT_BRANCH"
}

@test "dolt diff other" {
    dolt diff other
    run dolt diff other
    [ "$status" -eq 0 ]
    [[ "${lines[3]}"  =~ 'CREATE TABLE `abc` ('   ]] || false
    [[ "${lines[4]}"  =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "${lines[5]}"  =~ "\`a\` longtext"         ]] || false
    [[ "${lines[6]}"  =~ "\`b\` double"           ]] || false
    [[ "${lines[7]}"  =~ "-  \`w\` bigint"        ]] || false
    [[ "${lines[8]}"  =~ "-  \`z\` bigint"        ]] || false
    [[ "${lines[9]}"  =~ "+  \`x\` bigint"        ]] || false
    [[ "${lines[10]}" =~ "+  \`y\` bigint"        ]] || false
    [[ "${lines[11]}" =~ 'PRIMARY KEY (`pk`)'     ]] || false
    [[ "${lines[12]}" =~ ")"                      ]] || false
    [[ "${lines[13]}" =~ "+-----+----+------+-----+------+------+------+------+" ]] || false
    [[ "${lines[14]}" =~ "|  <  | pk | a    | b   |      |      | w    | z    |" ]] || false
    [[ "${lines[15]}" =~ "|  >  | pk | a    | b   | x    | y    |      |      |" ]] || false
    [[ "${lines[16]}" =~ "+-----+----+------+-----+------+------+------+------+" ]] || false
    [[ "${lines[17]}" =~ "|  <  | 0  | asdf | 1.1 | NULL | NULL | 0    | 122  |" ]] || false
    [[ "${lines[18]}" =~ "|  >  | 0  | asdf | 1.1 | 0    | 121  | NULL | NULL |" ]] || false
    [[ "${lines[19]}" =~ "|  -  | 1  | asdf | 1.1 | NULL | NULL | 0    | 122  |" ]] || false
    [[ "${lines[20]}" =~ "|  +  | 2  | asdf | 1.1 | 0    | 121  | NULL | NULL |" ]] || false
    [[ "${lines[21]}" =~ "|  +  | 3  | data | 1.1 | 0    | 121  | NULL | NULL |" ]] || false
    [[ "${lines[22]}" =~ "|  -  | 4  | data | 1.1 | NULL | NULL | 0    | 122  |" ]] || false
}

@test "big table" {
    run dolt sql -q "SELECT count(*) FROM big;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1000" ]] || false

    dolt sql -q "DELETE FROM big WHERE pk IN (71, 331, 881)"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "|  -  | 71  |" ]] || false
    [[ "$output" =~ "|  -  | 331 |" ]] || false
    [[ "$output" =~ "|  -  | 881 |" ]] || false

    run dolt sql -q "SELECT count(*) FROM big;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "997" ]] || false

    dolt sql -q "INSERT INTO big VALUES (1001, 'foo'), (1002, 'bar'), (1003, 'baz');"
    run dolt sql -q "SELECT count(*) FROM big;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1000" ]] || false

    dolt commit -am "inserted, deleted some rows"
}

@test "dolt merge other into $DEFAULT_BRANCH" {
    # throws a conflict
    dolt merge other
}

@test "dolt table import" {
    run dolt table import -c -pk=pk abc2 abc.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    dolt sql -q 'drop table abc2'
}

@test "dolt_schemas" {
    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "| type | name  | fragment             |" ]] || false
    [[ "${lines[2]}" =~ "+------+-------+----------------------+" ]] || false
    [[ "${lines[3]}" =~ "| view | view1 | SELECT 2+2 FROM dual |" ]] || false
    run dolt sql -q 'select * from view1'
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2+2" ]] || false
    [[ "${lines[2]}" =~ "-----" ]] || false
    [[ "${lines[3]}" =~ " 4 " ]] || false
}
