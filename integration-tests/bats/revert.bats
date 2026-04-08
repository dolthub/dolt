#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/local-remote.bash

setup() {
    setup_common

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT)"
    dolt commit -Am "Created table"
    dolt sql -q "INSERT INTO test VALUES (1, 1)"
    dolt commit -am "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 2)"
    dolt commit -am "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt commit -am "Inserted 3"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "revert: HEAD" {
    dolt revert HEAD
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: HEAD~1" {
    dolt revert HEAD~1
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: HEAD & HEAD~1" {
    dolt revert HEAD HEAD~1
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "revert: has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    run dolt revert HEAD
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "revert: conflicts" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt commit -am "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt commit -am "Updated 4"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false
    [[ "$output" =~ "data conflicts" ]] || false
    [[ "$output" =~ "dolt revert --continue" ]] || false
}

@test "revert: constraint violations" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt sql -q "DELETE FROM child WHERE pk = 2"
    dolt add -A
    dolt commit -m "MC2"
    dolt sql -q "DELETE FROM parent WHERE pk = 20"
    dolt add -A
    dolt commit -m "MC3"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false
    [[ "$output" =~ "constraint violations" ]] || false
    [[ "$output" =~ "dolt revert --continue" ]] || false
}

@test "revert: --abort after conflict restores state" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt commit -Am "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt commit -Am "Updated 4"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    run dolt revert --abort
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert aborted" ]] || false

    # Conflicts should be gone and working set should be clean
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false

    run dolt status
    [ "$status" -eq "0" ]
    [[ "$output" =~ "nothing to commit" ]] || false
}

@test "revert: --continue after resolving conflict" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # Resolve the conflict by deleting the conflicting row
    dolt conflicts resolve --ours test
    dolt add test

    run dolt revert --continue
    [ "$status" -eq "0" ]

    # Verify a new revert commit was created
    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false
}

@test "revert: --abort with no revert in progress" {
    run dolt revert --abort
    [ "$status" -eq "1" ]
    [[ "$output" =~ "no revert in progress" ]] || false
}

@test "revert: --continue with no revert in progress" {
    run dolt revert --continue
    [ "$status" -eq "1" ]
    [[ "$output" =~ "no revert in progress" ]] || false
}

@test "revert: multi-commit stops at first conflict" {
    dolt sql -q "INSERT INTO test VALUES (4, 4);"
    dolt commit -am "Inserted 4"
    dolt sql -q "UPDATE test SET v1=5 where pk=4;"
    dolt commit -am "Updated 4"
    dolt sql -q "UPDATE test SET v1=6 where pk=4;"
    dolt commit -am "Updated 4 (again)"

    # HEAD (Updated 4 (again)) reverts cleanly, but HEAD~2 (Inserted 4) conflicts, because that row
    # is now (4,5) instead of (4,4), so a conflict is generated that must be manually resolved.
    run dolt revert HEAD HEAD~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # HEAD was already reverted cleanly before the conflict
    run dolt log -n 2 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false
}

@test "revert: --abort after multi-commit revert restores original HEAD" {
    dolt sql -q "INSERT INTO test VALUES (4, 4);"
    dolt commit -am "Inserted 4"
    dolt sql -q "UPDATE test SET v1=5 where pk=4;"
    dolt commit -am "Updated 4"
    dolt sql -q "UPDATE test SET v1=6 where pk=4;"
    dolt commit -am "Updated 4 (again)"

    # Revert two commits: HEAD (Updated 4 (again)) reverts cleanly creating a commit,
    # then HEAD~2 (Inserted 4) conflicts. After --abort, HEAD should be back to
    # "Updated 4 (again)" — not the intermediate "Revert" commit.
    run dolt revert HEAD HEAD~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # HEAD advanced because the first revert (HEAD) succeeded
    run dolt log -n 1 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false

    run dolt revert --abort
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert aborted" ]] || false

    # After abort, HEAD should be back to the pre-series commit
    run dolt log -n 1 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Updated 4 (again)" ]] || false

    # No conflicts should remain
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
}

@test "revert: too far back" {
    run dolt revert HEAD~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "revert: init commit" {
    run dolt revert HEAD~4
    [ "$status" -ne "0" ]
    [[ "$output" =~ "cannot revert commit with no parents" ]] || false
}

@test "revert: invalid hash" {
    run dolt revert aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "revert: HEAD with --author parameter" {
    dolt revert HEAD --author "john <johndoe@gmail.com>"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Author: john <johndoe@gmail.com>" ]] || false
}

@test "revert: HEAD & HEAD~1 with --author parameter" {
    dolt revert HEAD HEAD~1 --author "john <johndoe@gmail.com>"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Author: john <johndoe@gmail.com>" ]] || false
}

@test "revert: PK tag changes but name and type match" {
    # don't run this test in remote-engine mode, since we have
    # calls to dolt schema update-tag that will directly
    skip_if_remote

    dolt sql <<"SQL"
CREATE TABLE pk_tag_test (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO pk_tag_test VALUES (1, 1), (2, 2);
SQL
    dolt commit -Am "created pk_tag_test with two rows"

    dolt sql -q "INSERT INTO pk_tag_test VALUES (3, 3)"
    dolt commit -am "inserted row 3"

    # Manually change the tag of the primary key column to simulate a tag mismatch
    # between historical commits and the current schema
    dolt schema update-tag pk_tag_test pk 99999
    dolt commit -am "changed pk column tag"

    # Revert the commit that inserted row 3; the PK tag in that commit differs
    # from the current schema tag, so mapping must fall back to name+type matching
    run dolt revert HEAD~1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM pk_tag_test ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD" {
    dolt sql -q "call dolt_revert('HEAD')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD~1" {
    dolt sql -q "call dolt_revert('HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD & HEAD~1" {
    dolt sql -q "call dolt_revert('HEAD', 'HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "revert: SQL has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    run dolt sql -q "call dolt_revert('HEAD')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "revert: SQL conflicts, @@autocommit=1" {
    dolt sql -q "INSERT INTO test VALUES (4, 4);"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5);"
    dolt add -A
    dolt commit -m "Updated 4"
    # With @@autocommit=1 and no allow vars, conflicts cause an error.
    run dolt sql -q "call dolt_revert('HEAD~1')" -r=csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Merge conflict detected, @autocommit transaction rolled back." ]] || false
    [[ "$output" =~ "@autocommit must be disabled so that merge conflicts can be resolved" ]] || false
}

@test "revert: SQL conflicts, @@autocommit=0" {
    dolt sql -q "INSERT INTO test VALUES (4, 4);"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5);"
    dolt add -A
    dolt commit -m "Updated 4"
    # SQL procedure returns status 0 with conflict counts in the result row
    run dolt sql -q "set @@autocommit=0; call dolt_revert('HEAD~1');" -r=csv
    [ "$status" -eq "0" ]
    # Result row: hash, data_conflicts, schema_conflicts, constraint_violations
    # data_conflicts should be 1
    [[ "$output" =~ ",1,0,0" ]] || false
}


@test "revert: SQL constraint violations, @@autocommit=0" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt commit -Am "MC1"
    dolt sql -q "DELETE FROM child WHERE pk = 2"
    dolt commit -am "MC2"
    dolt sql -q "DELETE FROM parent WHERE pk = 20"
    dolt commit -am "MC3"
    # SQL procedure returns status 0 with constraint violation count in the result row
    run dolt sql -q "set @@autocommit=0; call dolt_revert('HEAD~1')" -r=csv
    [ "$status" -eq "0" ]
    # constraint_violations should be 1
    [[ "$output" =~ ",0,0,1" ]] || false
}

@test "revert: SQL --abort and --continue workflow" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"

    # Start revert that will conflict. Use dolt_allow_commit_conflicts so the merge state
    # is persisted to disk and --abort works in the next dolt sql -q invocation.
    run dolt sql -q "set @@dolt_allow_commit_conflicts=1; call dolt_revert('HEAD~1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",1,0,0" ]] || false

    # Abort to restore clean state
    run dolt sql -q "call dolt_revert('--abort')" -r=csv
    [ "$status" -eq "0" ]

    # Verify no conflicts remain
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
}

@test "revert: SQL --continue after resolving conflict" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"

    # Start revert that will conflict. Use dolt_allow_commit_conflicts so the merge state
    # is persisted to disk and --continue works in a subsequent dolt sql -q invocation.
    run dolt sql -q "set @@dolt_allow_commit_conflicts=1; call dolt_revert('HEAD~1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",1,0,0" ]] || false

    # Resolve conflict by taking ours
    dolt conflicts resolve --ours test
    dolt add test

    # Continue the revert
    run dolt sql -q "call dolt_revert('--continue')" -r=csv
    [ "$status" -eq "0" ]

    # Verify revert commit was created
    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false
}

@test "revert: SQL --abort after multi-commit revert restores original HEAD" {
    dolt sql -q "INSERT INTO test VALUES (4, 4);"
    dolt commit -am "Inserted 4"
    dolt sql -q "UPDATE test SET v1=5 where pk=4;"
    dolt commit -am "Updated 4"
    dolt sql -q "UPDATE test SET v1=6 where pk=4;"
    dolt commit -am "Updated 4 (again)"

    # Revert two commits: HEAD reverts cleanly (commit created), HEAD~2 conflicts.
    # Use dolt_allow_commit_conflicts so the merge state is persisted for --abort.
    run dolt sql -q "set @@dolt_allow_commit_conflicts=1; call dolt_revert('HEAD', 'HEAD~2')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",1,0,0" ]] || false

    # The first revert (HEAD) succeeded and advanced HEAD
    run dolt log -n 1 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false

    # Abort restores HEAD to the pre-series commit
    run dolt sql -q "call dolt_revert('--abort')" -r=csv
    [ "$status" -eq "0" ]

    run dolt log -n 1 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Updated 4 (again)" ]] || false

    # No conflicts should remain
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
}

@test "revert: --continue called multiple times for multi-commit series" {
    # Commit A: change pk=1. Will revert cleanly.
    dolt sql -q "UPDATE test SET v1=10 WHERE pk=1;"
    dolt commit -am "change pk1"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_A=${lines[1]}
    # Commit B: change pk=2. Will conflict because of clobber below.
    dolt sql -q "UPDATE test SET v1=20 WHERE pk=2;"
    dolt commit -am "change pk2"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_B=${lines[1]}
    # Commit C: change pk=3. Will conflict because of clobber below.
    dolt sql -q "UPDATE test SET v1=30 WHERE pk=3;"
    dolt commit -am "change pk3"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_C=${lines[1]}
    # Clobber: overwrite pk=2 and pk=3 so reverting B and C will produce conflicts.
    dolt sql -q "UPDATE test SET v1=21 WHERE pk=2;"
    dolt sql -q "UPDATE test SET v1=31 WHERE pk=3;"
    dolt commit -am "clobber pk2 and pk3"

    # A reverts cleanly (commit created). B conflicts → stop.
    run dolt revert "$HASH_A" "$HASH_B" "$HASH_C"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # A's revert committed: pk=1 restored to 1.
    run dolt sql -q "SELECT v1 FROM test WHERE pk=1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # B's revert left a conflict.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # Resolve B's conflict: keep the clobber value for pk=2.
    dolt conflicts resolve --ours test
    dolt sql -q "UPDATE test SET v1=21 WHERE pk=2;"
    dolt add test

    # First --continue: commits B's revert, automatically tries C, C conflicts → stop.
    run dolt revert --continue
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # B's revert is now committed.
    run dolt log -n 2 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert \"change pk2\"" ]] || false
    [[ "$output" =~ "Revert \"change pk1\"" ]] || false

    # C's revert left a conflict.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # Resolve C's conflict: keep the clobber value for pk=3.
    dolt conflicts resolve --ours test
    dolt sql -q "UPDATE test SET v1=31 WHERE pk=3;"
    dolt add test

    # Second --continue: commits C's revert, series complete.
    run dolt revert --continue
    [ "$status" -eq "0" ]

    # All three revert commits present.
    run dolt log -n 3 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert \"change pk3\"" ]] || false
    [[ "$output" =~ "Revert \"change pk2\"" ]] || false
    [[ "$output" =~ "Revert \"change pk1\"" ]] || false

    # No conflicts remaining.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
}

@test "revert: SQL --continue called multiple times for multi-commit series" {
    # Commit A: change pk=1. Will revert cleanly.
    dolt sql -q "UPDATE test SET v1=10 WHERE pk=1;"
    dolt commit -am "change pk1"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_A=${lines[1]}
    # Commit B: change pk=2. Will conflict because of clobber below.
    dolt sql -q "UPDATE test SET v1=20 WHERE pk=2;"
    dolt commit -am "change pk2"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_B=${lines[1]}
    # Commit C: change pk=3. Will conflict because of clobber below.
    dolt sql -q "UPDATE test SET v1=30 WHERE pk=3;"
    dolt commit -am "change pk3"
    run dolt sql -q "SELECT dolt_hashof('HEAD');" -r=csv
    HASH_C=${lines[1]}
    # Clobber: overwrite pk=2 and pk=3 so reverting B and C will produce conflicts.
    dolt sql -q "UPDATE test SET v1=21 WHERE pk=2;"
    dolt sql -q "UPDATE test SET v1=31 WHERE pk=3;"
    dolt commit -am "clobber pk2 and pk3"

    # A reverts cleanly (commit created). B conflicts → stop.
    # Use dolt_allow_commit_conflicts so the merge state is persisted for --continue.
    run dolt sql -q "set @@dolt_allow_commit_conflicts=1; call dolt_revert('$HASH_A', '$HASH_B', '$HASH_C')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",1,0,0" ]] || false

    # A's revert committed: pk=1 restored to 1.
    run dolt sql -q "SELECT v1 FROM test WHERE pk=1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # B's revert left a conflict.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # Resolve B's conflict.
    dolt conflicts resolve --ours test
    dolt sql -q "UPDATE test SET v1=21 WHERE pk=2;"
    dolt add test

    # First --continue: commits B's revert, automatically tries C, C conflicts → stop.
    # Use dolt_allow_commit_conflicts so the merge state is persisted for the next --continue.
    run dolt sql -q "set @@dolt_allow_commit_conflicts=1; call dolt_revert('--continue')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",1,0,0" ]] || false

    # B's revert is now committed.
    run dolt log -n 2 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert \"change pk2\"" ]] || false
    [[ "$output" =~ "Revert \"change pk1\"" ]] || false

    # C's revert left a conflict.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1" ]] || false

    # Resolve C's conflict.
    dolt conflicts resolve --ours test
    dolt sql -q "UPDATE test SET v1=31 WHERE pk=3;"
    dolt add test

    # Second --continue: commits C's revert, series complete.
    run dolt sql -q "call dolt_revert('--continue')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ ",0,0,0" ]] || false

    # All three revert commits present.
    run dolt log -n 3 --oneline
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert \"change pk3\"" ]] || false
    [[ "$output" =~ "Revert \"change pk2\"" ]] || false
    [[ "$output" =~ "Revert \"change pk1\"" ]] || false

    # No conflicts remaining.
    run dolt sql -q "SELECT count(*) FROM dolt_conflicts" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "0" ]] || false
}

@test "revert: SQL --abort with no revert in progress" {
    run dolt sql -q "call dolt_revert('--abort')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "no revert in progress" ]] || false
}

@test "revert: SQL --continue with no revert in progress" {
    run dolt sql -q "call dolt_revert('--continue')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "no revert in progress" ]] || false
}

@test "revert: SQL too far back" {
    run dolt sql -q "call dolt_revert('HEAD~10')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "revert: SQL revert init commit" {
    run dolt sql -q "call dolt_revert('HEAD~4')"
    [ "$status" -ne "0" ]
    [[ "$output" =~ "cannot revert commit with no parents" ]] || false
}

@test "revert: SQL invalid hash" {
    run dolt sql -q "call dolt_revert('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "revert: SQL HEAD with author" {
    dolt sql -q "call dolt_revert('HEAD', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}

@test "revert: SQL HEAD & HEAD~1 with author" {
    dolt sql -q "call dolt_revert('HEAD', 'HEAD~1', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}

@test "revert: --continue with ignored table in working set" {
    # Commit the ignore pattern first so reverting later commits doesn't remove it.
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('ignored_*', 1)"
    dolt sql -q "CREATE TABLE ignored_1 (id INT PRIMARY KEY);"
    dolt commit -Am "Add ignore pattern"
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt commit -am "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt commit -am "Updated 4"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Automatic revert failed" ]] || false

    # Resolve the conflict
    dolt conflicts resolve --ours test
    dolt add test

    # Create another ignored table in the working set
    dolt sql -q "CREATE TABLE ignored_2 (id INT PRIMARY KEY)"

    # --continue should succeed despite the ignored table in the working set
    run dolt revert --continue
    [ "$status" -eq "0" ]

    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Revert" ]] || false
}
