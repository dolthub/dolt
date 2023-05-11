#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "chunk-journal: assert chunk journal index is correctly bootstrapped" {
    dolt sql -q "create table t (pk int primary key, c0 text);"
    dolt commit -Am "new table t"

    # chunk journal index is only populated after a sufficient number of chunk
    # records have been written to the journal, see go/store/nbs/journal_writer.go
    echo "insert into t values" > import.sql
    for i in {1..16384}
    do
        echo "  ($i,'$i')," >> import.sql
    done
    echo "  (16385,'16385');" >> import.sql

    dolt sql < import.sql

    # read the database
    dolt status
    [ -s ".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]
    [ -s ".dolt/noms/journal.idx" ]

    # write the database
    dolt checkout -b newbranch
    [ -s ".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]
    [ -s ".dolt/noms/journal.idx" ]
}
