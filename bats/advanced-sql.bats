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

@test "select the same column twice using aliasing" {
    run dolt sql -q "select pk,c1 as foo,c1 as bar from one_pk"
    [ "$status" -eq 0 ]
    skip "This breaks right now. Reports one column as NULL"
    [[ ! "$output" =~ "<NULL>" ]] || false
}

@test "select using table aliases" {
    run dolt sql -q "select pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 0 ]
    skip "This breaks right now. Reports one column as NULL"
    [[ ! "$output" =~ "<NULL>" ]] || false
}