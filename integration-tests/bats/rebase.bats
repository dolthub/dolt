#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql -q "CREATE table t1 (pk int primary key, c int);"
    dolt commit -Am "main commit 1"
    dolt branch b1
    dolt sql -q "INSERT INTO t1 VALUES (1,1);"
    dolt commit -am "main commit 2"

    dolt checkout b1
    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt commit -Am "b1 commit 1"

    dolt checkout main
}

teardown() {
    assert_feature_version
    teardown_common
}

# sets up the EDITOR env var with a script that takes the input file from
# the process invoking the editor and copies it to the editor-input.txt
# file for tests to check, and then copies the file specifeid as an argument
# to this function, as the output for the editor, sent back to the process
# that invoked the editor.
setupCustomEditorScript() {
    touch rebaseScript.sh
    echo "#!/bin/bash" >> rebaseScript.sh
    echo "cp \$1 editor-input.txt" >> rebaseScript.sh
    if [ $# -eq 1 ]; then
      echo "mv $1 \$1" >> rebaseScript.sh
    fi
    chmod +x rebaseScript.sh
    export EDITOR=$PWD/rebaseScript.sh
    export DOLT_TEST_FORCE_OPEN_EDITOR="1"
}

@test "rebase: no rebase in progress errors" {
    run dolt rebase --abort
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false
}

@test "rebase: non-interactive rebase works" {
    run dolt sql -r csv -q "select * from dolt_branch_status('main', 'b1');"
    [ "$status" -eq 0 ]
    # main and b1 have diverged by 1 commit each
    [[ "$output" =~ "b1,1,1" ]] || false

    run dolt rebase b1
    [ "$status" -eq 0 ]

    # Main should be 1 ahead of b1 now
    run dolt sql -r csv -q "select * from dolt_branch_status('main', 'b1');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1,0,1" ]] || false
}

@test "rebase: bad args" {
    run dolt rebase -i
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not enough args" ]] || false

    run dolt rebase -i main b1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "rebase takes at most one positional argument" ]] || false

    run dolt rebase --abrot
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: unknown option \`abrot'" ]] || false

    run dolt rebase -i foo
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch not found: foo" ]] || false
}

@test "rebase: cannot rebase with dirty working set" {
    dolt sql -q "INSERT INTO t1 VALUES (2,2);"
    run dolt rebase -i b1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot start a rebase with uncommitted changes" ]] || false
}

@test "rebase: cannot rebase during active merge" {
    dolt checkout b1
    dolt sql -q "INSERT INTO t1 VALUES (1,2);"
    dolt add t1
    dolt commit -m "b1 commit 2"

    run dolt merge main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Automatic merge failed" ]] || false

    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unable to start rebase while a merge is in progress – abort the current merge before proceeding" ]] || false
}

@test "rebase: rebase working branch already exists" {
    dolt checkout b1
    dolt branch dolt_rebase_b1

    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: A branch named 'dolt_rebase_b1' already exists." ]] || false
}

@test "rebase: verify custom script" {
    setupCustomEditorScript "rebasePlan.txt"

    dolt checkout b1
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT1=${lines[0]:12:32}

    touch rebasePlan.txt
    echo "pick $COMMIT1 b1 commit 1" >> rebasePlan.txt

    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1 commit 1" ]] || false
    [[ "$output" =~ "main commit 2" ]] || false
}

@test "rebase: basic rebase" {
    setupCustomEditorScript

    dolt checkout b1
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main commit 2" ]] || false
}

# bats test_tags=no_lambda
# skip bats on lambda, since we don't have the pcre2grep utility there
@test "rebase: multi-line commit messages" {
    setupCustomEditorScript

    # Create a multi-line commit message
    dolt checkout b1
    dolt commit --allow-empty -m "multi
line
commit
message"

    # Run rebase (with the default plan, custom editor makes no changes)
    run dolt rebase --empty=keep -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Assert that the newlines were removed in the rebase plan editor
    grep "multi line commit message" editor-input.txt

    # Assert that the commit log still shows the multi-line message
    run dolt log -n1
    [ "$status" -eq 0 ]
    echo "$output" > tmp.out
    pcre2grep -nM "multi\s*\R+\s*line\s*\R+\s*commit\s*\R+\s*message" tmp.out
}

@test "rebase: failed rebase will abort and clean up" {
    setupCustomEditorScript "invalidRebasePlan.txt"
    dolt checkout b1

    touch invalidRebasePlan.txt
    echo "foo" >> invalidRebasePlan.txt
    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid line 0: foo" ]] || false

    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false

    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false
}

@test "rebase: invalid rebase plan" {
    setupCustomEditorScript "invalidRebasePlan.txt"

    dolt checkout b1

    touch invalidRebasePlan.txt
    echo "foo" >> invalidRebasePlan.txt
    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid line 0: foo" ]] || false

    touch invalidRebasePlan.txt
    echo "pick foo main commit 1" >> invalidRebasePlan.txt
    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid commit hash: foo" ]] || false
}

@test "rebase: empty rebase plan aborts the rebase" {
    setupCustomEditorScript "emptyRebasePlan.txt"
    touch emptyRebasePlan.txt
    echo "# " >> emptyRebasePlan.txt
    echo "# commented out lines don't count" >> emptyRebasePlan.txt

    dolt checkout b1
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rebase aborted" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "main commit 2" ]] || false

    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false
}

@test "rebase: multi step rebase" {
    setupCustomEditorScript "multiStepPlan.txt"

    dolt checkout b1
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT1=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (1);"
    dolt commit -am "b1 commit 2"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT2=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (2);"
    dolt commit -am "b1 commit 3"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT3=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (3);"
    dolt commit -am "b1 commit 4"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT4=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (4);"
    dolt commit -am "b1 commit 5"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT5=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (5);"
    dolt commit -am "b1 commit 6"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT6=${lines[0]:12:32}

    dolt sql -q "insert into t2 values (6);"
    dolt commit -am "b1 commit 7"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT7=${lines[0]:12:32}

    touch multiStepPlan.txt
    echo "pick $COMMIT1 b1 commit 1" >> multiStepPlan.txt
    echo "squash $COMMIT2 b1 commit 2" >> multiStepPlan.txt
    echo "squash $COMMIT3 b1 commit 3" >> multiStepPlan.txt
    echo "drop $COMMIT4 b1 commit 4" >> multiStepPlan.txt
    echo "reword $COMMIT5 reworded!" >> multiStepPlan.txt
    echo "fixup $COMMIT6 b1 commit 6" >> multiStepPlan.txt
    echo "pick $COMMIT7 b1 commit 7" >> multiStepPlan.txt

    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt show head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1 commit 7" ]] || false

    run dolt show head~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "reworded!" ]] || false

    run dolt show head~2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1 commit 1" ]] || false
    [[ "$output" =~ "b1 commit 2" ]] || false
    [[ "$output" =~ "b1 commit 3" ]] || false

    run dolt show head~3
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main commit 2" ]] || false
}

@test "rebase: non-standard plan changes" {
    setupCustomEditorScript "nonStandardPlan.txt"

    dolt checkout -b b2
    dolt sql -q "CREATE table t3 (pk int primary key);"
    dolt add t3
    dolt commit -m "b2 commit 1"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT1=${lines[0]:12:32}

    dolt sql -q "insert into t3 values (1);"
    dolt commit -am "b2 commit 2"
    dolt sql -q "insert into t3 values (2);"
    dolt commit -am "b2 commit 3"

    dolt checkout b1
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT2=${lines[0]:12:32}

    touch nonStandardPlan.txt
    echo "pick $COMMIT1 b2 commit 1" >> nonStandardPlan.txt
    echo "pick $COMMIT2 b1 commit 1" >> nonStandardPlan.txt

    dolt checkout b2
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b2" ]] || false

    run dolt show head
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1 commit 1" ]] || false

    run dolt show head~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b2 commit 1" ]] || false

    run dolt show head~2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main commit 2" ]] || false
}

@test "rebase: rebase skips merge commits" {
    setupCustomEditorScript

    dolt checkout b1
    dolt merge main -m "b1 merge commit"
    dolt sql -q "insert into t2 values (1);"
    dolt commit -am "b1 commit 2"

    dolt checkout main
    dolt sql -q "insert into t1 values (2,2);"
    dolt commit -am "main commit 3"
    dolt checkout b1

    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1 commit 2" ]] || false
    [[ "$output" =~ "b1 commit 1" ]] || false
    [[ "$output" =~ "main commit 3" ]] || false
    ! [[ "$output" =~ "b1 merge commit" ]] || false
}

@test "rebase: rebase with data conflict" {
    setupCustomEditorScript

    dolt checkout b1
    dolt sql -q "INSERT INTO t1 VALUES (1,2);"
    dolt commit -am "b1 commit 2"

    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "b1 commit 2" ]] || false


    # Assert that we are on the rebase working branch (not the branch being rebased)
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Review and resolve the data conflict
    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | ours   | 1  | 1 | NULL | NULL | NULL | NULL |" ]] || false
    [[ "$output" =~ "+  | theirs | 1  | 2 | NULL | NULL | NULL | NULL |" ]] || false
    dolt conflicts resolve --theirs t1
    dolt add t1

    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Assert that we are back on the branch being rebased
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* b1" ]] || false
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false
}

@test "rebase: rebase with multiple data conflicts" {
    setupCustomEditorScript
    dolt sql -q "INSERT INTO t1 VALUES (2,200);"
    dolt commit -am "main commit 3"

    dolt checkout b1
    dolt sql -q "INSERT INTO t1 VALUES (1,2);"
    dolt commit -am "b1 commit 2"
    dolt sql -q "INSERT INTO t1 VALUES (2,3);"
    dolt commit -am "b1 commit 3"

    # Start the rebase
    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "b1 commit 2" ]] || false

    # Assert that we are on the rebase working branch now (not the branch being rebased)
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Review and resolve the first data conflict
    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | ours   | 1  | 1 | NULL | NULL | NULL | NULL |" ]] || false
    [[ "$output" =~ "+  | theirs | 1  | 2 | NULL | NULL | NULL | NULL |" ]] || false
    dolt conflicts resolve --theirs t1

    # Without staging the changed tables, trying to continue the rebase results in an error
    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot continue a rebase with unstaged changes" ]] || false

    # Stage the tables and then continue the rebase and hit the second data conflict
    dolt add t1
    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "b1 commit 3" ]] || false

    # Assert that we are still on the rebase working branch
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Review and resolve the second data conflict
    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | ours   | 2  | 200 | NULL | NULL | NULL | NULL |" ]] || false
    [[ "$output" =~ "+  | theirs | 2  | 3   | NULL | NULL | NULL | NULL |" ]] || false
    dolt conflicts resolve --ours t1

    # Finish the rebase
    dolt add t1
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Assert that we are back on the branch that was rebased, and that the rebase working branch is gone
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1" ]] || false
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false
}

@test "rebase: rebase with schema conflicts aborts" {
    setupCustomEditorScript

    dolt checkout b1
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN c varchar(100);"
    dolt commit -am "b1 commit 2"

    run dolt rebase -i main
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "the rebase has been automatically aborted" ]] || false

    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no rebase in progress" ]] || false

    run dolt branch
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false
}

@test "rebase: rebase with commits that become empty" {
    setupCustomEditorScript

    # Apply the same change to b1 that was applied to main in it's most recent commit
    # and tag the tip of b1, so we can go reset back to this commit
    dolt checkout b1
    dolt sql -q "INSERT INTO t1 VALUES (1,1);"
    dolt commit -am "repeating change from main on b1"
    dolt tag testStartPoint

    # By default, dolt will drop the empty commit
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Make sure the commit that became empty doesn't appear in the commit log
    run dolt log
    [[ ! $output =~ "repeating change from main on b1" ]] || false

    # Reset back to the test start point and repeat the rebase with --empty=drop (the default)
    dolt reset --hard testStartPoint
    run dolt rebase -i --empty=drop main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Make sure the commit that became empty does NOT appear in the commit log
    run dolt log
    [[ ! $output =~ "repeating change from main on b1" ]] || false

    # Reset back to the test start point and repeat the rebase with --empty=keep
    dolt reset --hard testStartPoint
    run dolt rebase -i --empty=keep main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Make sure the commit that became empty appears in the commit log
    run dolt log
    [[ $output =~ "repeating change from main on b1" ]] || false
}

@test "rebase: edit action basic functionality" {
    # Get the commit hash for b1 commit 1 using the established pattern
    dolt checkout b1
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT1=${lines[0]:12:32}
    
    # Create a rebase plan file with an edit action
    touch rebase_plan.txt
    echo "edit $COMMIT1 b1 commit 1" >> rebase_plan.txt

    setupCustomEditorScript rebase_plan.txt

    # Start interactive rebase with edit action - should pause for editing
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "edit action paused at commit" ]] || false
    [[ "$output" =~ "You can now modify the working directory" ]] || false

    # Assert that we are on the rebase working branch (not the branch being rebased)
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Continue the rebase after the edit pause
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Assert that we are on the original working branch
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1" ]] || false
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false
}

@test "rebase: complex edit and conflict scenario" {
    # Setup: Create conflicting data on main (for commit 2)
    dolt sql -q "INSERT INTO t1 VALUES (10, 100);"
    dolt commit -am "main conflicting data"

    # Setup: Create 4 commits on b1 to rebase
    dolt checkout b1

    # Commit 1: Will be edited (no conflict with main)
    dolt sql -q "INSERT INTO t1 VALUES (5, 50);"
    dolt commit -am "b1 commit 1 - to edit"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT1=${lines[0]:12:32}

    # Commit 2: Will conflict with main
    dolt sql -q "INSERT INTO t1 VALUES (10, 200);"  # Conflicts with main's (10, 100)
    dolt commit -am "b1 commit 2 - will conflict"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT2=${lines[0]:12:32}

    # Commit 3: Clean apply after conflict
    dolt sql -q "INSERT INTO t1 VALUES (20, 300);"
    dolt commit -am "b1 commit 3 - clean apply"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT3=${lines[0]:12:32}

    # Commit 4: Will be edited at the end
    dolt sql -q "INSERT INTO t1 VALUES (30, 400);"
    dolt commit -am "b1 commit 4 - edit at end"
    run dolt show head
    [ "$status" -eq 0 ]
    COMMIT4=${lines[0]:12:32}

    # Create rebase plan: edit, pick (conflict), pick, edit
    setupCustomEditorScript "complex_rebase_plan.txt"
    touch complex_rebase_plan.txt
    echo "edit $COMMIT1 b1 commit 1 - to edit" >> complex_rebase_plan.txt
    echo "pick $COMMIT2 b1 commit 2 - will conflict" >> complex_rebase_plan.txt
    echo "pick $COMMIT3 b1 commit 3 - clean apply" >> complex_rebase_plan.txt
    echo "edit $COMMIT4 b1 commit 4 - edit at end" >> complex_rebase_plan.txt

    # Start the rebase - should pause at first edit
    run dolt rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "edit action paused at commit" ]] || false

    # Assert we're on the rebase working branch
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Make changes during edit pause and add another commit
    dolt sql -q "UPDATE t1 SET c = 55 WHERE pk = 5;"
    dolt commit -a --amend -m "edit modification to commit 1"

    dolt sql -q "INSERT INTO t1 VALUES (15, 150);"
    dolt add t1
    dolt commit -m "edit modification to commit 1"
    
    # Continue rebase - should hit conflict on commit 2
    run dolt rebase --continue
    [ "$status" -eq 1 ]
    [[ "$output" =~ "data conflict detected while rebasing commit" ]] || false
    [[ "$output" =~ "b1 commit 2 - will conflict" ]] || false

    # Resolve the conflict
    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  | ours   | 10 | 100 | NULL | NULL | NULL | NULL |" ]] || false
    [[ "$output" =~ "+  | theirs | 10 | 200 | NULL | NULL | NULL | NULL |" ]] || false
    dolt conflicts resolve --theirs t1
    dolt add t1

    # Continue rebase - should proceed to commit 3 then pause at commit 4 edit
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "edit action paused at commit" ]] || false
    [[ "$output" =~ "b1 commit 4 - edit at end" ]] || false

    # We're still on the rebase working branch
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " dolt_rebase_b1 " ]] || false

    # Continue from edit pause - should complete successfully
    run dolt rebase --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    # Verify we're back on the original branch
    run dolt sql -q "select active_branch();"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "b1" ]] || false
    ! [[ "$output" =~ "dolt_rebase_b1" ]] || false

    # Count commits between main and HEAD - should be 5
    # (original 4 commits + 1 edit modification commit)
    run dolt log --oneline main..HEAD
    [ "$status" -eq 0 ]
    # Count the lines (each commit is one line)
    commit_count=$(echo "$output" | wc -l)
    [ "$commit_count" -eq 5 ]

    # Verify the data is correct after all operations
    run dolt sql -q "SELECT * FROM t1 ORDER BY pk;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1  | 1   |" ]] || false  # Original main data
    [[ "$output" =~ "| 5  | 55  |" ]] || false  # Modified during edit
    [[ "$output" =~ "| 10 | 200 |" ]] || false  # Conflict resolved to theirs
    [[ "$output" =~ "| 20 | 300 |" ]] || false  # From commit 3
    [[ "$output" =~ "| 30 | 400 |" ]] || false  # From commit 4

    # Verify no rebase state remains
    run dolt status
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "rebase in progress" ]] || false
}
