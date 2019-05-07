#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema one_pk
    dolt table create -s=$BATS_TEST_DIRNAME/helper/2pk5col-ints.schema two_pk
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30)"
    dolt sql -q "insert into two_pk (pk1,pk2,c1,c2,c3,c4,c5) values (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30)"
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "select from multiple tables" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk,two_pk where foo=bar"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
}

@test "ambiguous column name" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where c1=0"
    [ "$status" -eq 1 ]
    [ "$output" = "Ambiguous column: c1" ]
}

@test "select with and and or clauses" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where pk=0 and pk1=0 or pk2=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 13 ]
}

@test "select the same column twice using column aliases" {
    run dolt sql -q "select pk,c1 as foo,c1 as bar from one_pk"
    [ "$status" -eq 0 ]
    skip "This breaks right now. Reports one column as NULL"
    [[ ! "$output" =~ "<NULL>" ]] || false
}

@test "select same column twice using table aliases" {
    run dolt sql -q "select pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 0 ]
    skip "This breaks right now. Reports one column as NULL"
    [[ ! "$output" =~ "<NULL>" ]] || false
}

@test "basic inner join" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    first_join_output=$output
    run dolt sql -q "select pk,pk1,pk2 from two_pk join one_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [ "$output" = "$first_join_output" ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1 where pk=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on foo=bar"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on foo=bar where foo=10"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
}

@test "table name in the from clause and the join statement" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk join one_pk on pk=pk1"
    skip "Though weird we think this is valid but it panics right now. At least make it not panic."
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
}

@test "sql is null and is not null statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk from one_pk where c3 is null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3 is not null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ ! "$output" =~ "11" ]] || false
}

@test "sql addition and subtraction" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,0,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3-c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c2+c1<=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "11" ]] || false
}

@test "addition on both left and right side of comparison operator" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,0,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2+c1<=5-0"
    skip "This fails right now with a numeric value but non-nuneric column error"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
}

@test "select with in list" {
    run dolt sql -q "select pk from one_pk where c1 in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    run dolt sql -q "select pk from one_pk where c1 in (11,21)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    run dolt sql -q "select pk from one_pk where c1 not in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "3" ]] || false
    run dolt sql -q "select pk from one_pk where c1 not in (10,20) and c1 in (30)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "3" ]] || false
}

@test "sql parser does not support empty list" {
    run dolt sql -q "select pk from one_pk where c1 not in ()"
    [ $status -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "sql addition in join statement" {
    run dolt sql -q "select * from one_pk join two_pk on pk1-pk>0 and pk2<1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "20" ]] || false
}

@test "leave off table name in select" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk where c3 is null"
    [ $status -eq 1 ]
    skip "Bad error message for no table name"
    [[ "$output" =~ "Missing table name" ]] || false
    [[ ! "$output" =~ "Unknown table 'dual'" ]] || false
}