#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

BATS_TEST_TIMEOUT=50

# This function was used to create the dolt repo used for this test. It is not run during testing.
create_repo() {
    echo 1
}

setup() {
    cp -r $BATS_TEST_DIRNAME/performance-repo/ $BATS_TMPDIR/dolt-repo-$$
    cd $BATS_TMPDIR/dolt-repo-$$
}

@test "performance: merge with no schema change and no conflict" {
    dolt checkout full
    dolt checkout -b mod2
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 2 = 0"

    dolt add .
    dolt commit -m "Add mod2 rows"

    dolt checkout full
    dolt checkout -b mod3
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 3 = 0"

    dolt add .
    dolt commit -m "Add mod3 rows"

    run dolt merge mod2
    log_status_eq 0
}

@test "performance: merge with no schema change and conflict" {
    pwd
    ls -al
    dolt checkout full
    dolt checkout -b mod2
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 2 = 0"

    dolt add .
    dolt commit -m "Add mod2 rows"

    dolt checkout full
    dolt checkout -b mod3
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 3 = 0"
    dolt sql -q 'update t set c0 = "conflict" where pk = 91'

    dolt add .
    dolt commit -m "Add mod3 rows"

    run dolt merge mod2
    log_status_eq 1
    [[ "$output" =~ "Merge conflict in t" ]] || false

    dolt conflicts resolve --ours t
    BATS_TEST_TIMEOUT=1
}


@test "performance: merge with schema change and no conflict" {
    dolt checkout full
    dolt checkout -b mod2
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 2 = 0"

    dolt add .
    dolt commit -m "Add mod2 rows"

    dolt sql -q "alter table t add column c1 int default 1"
    dolt add .
    dolt commit -m "Add column c1"

    dolt checkout full
    dolt checkout -b mod3
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 3 = 0"

    dolt add .
    dolt commit -m "Add mod3 rows"

    dolt sql -q "alter table t add column c2 int default 2"
    dolt add .
    dolt commit -m "Add column c2"

    run dolt merge mod2
    log_status_eq 0
}

@test "performance: merge with schema change and conflict" {
    dolt checkout full
    dolt checkout -b mod2
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 2 = 0"

    dolt add .
    dolt commit -m "Add mod2 rows"

    dolt sql -q "alter table t add column c1 int default 1"
    dolt add .
    dolt commit -m "Add column c1"

    dolt checkout full
    dolt checkout -b mod3
    dolt reset --soft HEAD^

    dolt sql -q "delete from t where pk % 3 = 0"
    dolt sql -q 'update t set c0 = "conflict" where pk = 91'

    dolt add .
    dolt commit -m "Add mod3 rows"

    dolt sql -q "alter table t add column c2 int default 2"
    dolt add .
    dolt commit -m "Add column c2"

    run dolt merge mod2
    log_status_eq 1

    dolt conflicts resolve --ours t
}