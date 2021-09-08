#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
    setup_common
    cd $BATS_TMPDIR
    mkdir remotes-$$
    mkdir remotes-$$/empty
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    remotesrv_pid=$!
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
}

teardown() {
    teardown_common
    kill $remotesrv_pid
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "sql-remotes-server: push with only one argument" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt sql -q "select dolt_push('test-remote')"
    [ "$status" -eq 1 ]
}

@test "sql-remotes-server: push and pull master branch from a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    # TODO dolt_push
    #run dolt sql -q "select dolt_push('test-remote', 'master')"
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt sql -q "select dolt_pull('test-remote')"
    [ "$status" -eq 0 ]
    #[[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "sql-remotes-server: push and pull non-master branch from remote" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    # TODO dolt_push
    run dolt sql -q "select dolt_push('test-remote', 'test-branch')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select dolt_pull('test-remote')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
}

@test "sql-remotes-server: push and pull from non-master branch and use --set-upstream" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt sql -q "select dolt_push('--set-upstream', 'test-remote', 'test-branch')"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic:" ]] || false
    dolt sql -q "create table test (pk int, c1 int, primary key(pk))"
    dolt add .
    dolt commit -m "Added test table"
    run dolt sql -q "select dolt_push()"
    [ "$status" -eq 0 ]
}

@test "sql-remotes-server: push and pull with docs from remote" {
    skip "todo pull docs?"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    echo "license-text" > LICENSE.md
    echo "readme-text" > README.md
    dolt add .
    dolt commit -m "test doc commit"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test doc commit" ]] || false

    cd ../../
    echo "updated-license" > LICENSE.md
    dolt add .
    dolt commit -m "updated license"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master

    cd dolt-repo-clones/test-repo
    echo "this text should remain after pull :p" > README.md
    run dolt sql -q "select dolt_pull()"
    [ "$status" -eq 0 ]
    #[[ "$output" =~ "Updating" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated license" ]] || false
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "updated-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "this text should remain after pull :p" ]] || false
}

@test "sql-remotes-server: push and pull tags to/from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd ../
    dolt tag v1 head
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'v1')"
    dolt push test-remote v1

    cd dolt-repo-clones/test-repo
    dolt sql -q "select dolt_pull()"
    [ "$status" -eq 0 ]
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "sql-remotes-server: tags are only pulled if their commit is pulled" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO  test VALUES (1),(2),(3);
SQL
    dolt add . && dolt commit -m "added table test"
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

     cd ../
    dolt tag v1 head -m "tag message"
    # TODO: dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'v1')"
    dolt push test-remote v1
    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (8),(9),(10)"
    dolt add . && dolt commit -m "added values on branch other"
    # TODO: dolt_push
    #dolt sql -q "select dolt_push('-u', 'test-remote', 'other')"
    dolt push -u test-remote other
    dolt tag other_tag head  -m "other message"
    # TODO: dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'other_tag')"
    dolt push test-remote other_tag

    cd dolt-repo-clones/test-repo
    run dolt sql -q "select dolt_pull()"
    [ "$status" -eq 0 ]
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ ! "$output" =~ "other_tag" ]] || false
    dolt fetch
    run dolt tag -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "tag message" ]] || false
    [[ "$output" =~ "other_tag" ]] || false
    [[ "$output" =~ "other message" ]] || false
}

@test "sql-remotes-server: dolt fetch" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "select dolt_push('test-remote', 'master')"
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch poop refs/heads/master:refs/remotes/poop/master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/poop
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/test-remote/poop" ]] || false
}

@test "sql-remotes-server: dolt fetch with docs" {
    skip "todo implement SQL push, pull, fetch"
    # Initial commit of docs on remote
    echo "initial-license" > LICENSE.md
    echo "initial-readme" > README.md
    dolt add .
    dolt commit -m "initial doc commit"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "select dolt_push('test-remote', 'master)"
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-readme" ]] || false
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-license" ]] || false

    # Clone the initial docs/repo into dolt-repo-clones/test-repo
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "initial-readme" ]] || false
    # Change the docs 
    echo "dolt-repo-clones-license" > LICENSE.md
    echo "dolt-repo-clones-readme" > README.md
    dolt add .
    dolt commit -m "dolt-repo-clones updated docs"

    # Go back to original repo, and change the docs again
    cd ../../
    echo "initial-license-updated" > LICENSE.md
    echo "initial-readme-updated" > README.md
    dolt add .
    dolt commit -m "update initial doc values in test-org/test-repo"

    # Go back to dolt-repo-clones/test-repo and fetch the test-remote
    cd dolt-repo-clones/test-repo
    run dolt fetch test-remote
    run cat LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt-repo-clones-license" ]] || false
    run cat README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt-repo-clones-readme" ]] || false
}

@test "sql-remotes-server: try to push a remote that is behind tip" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones/test-repo"
    run dolt sql -q "select dolt_push('origin', 'master')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Everything up-to-date" ]] || false
    dolt fetch
    run dolt sql -q "select dolt_push('origin', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "sql-remotes-server: generate a merge with no conflict with a remote branch" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:10',
  c1 BIGINT COMMENT 'tag:11',
  c2 BIGINT COMMENT 'tag:12',
  c3 BIGINT COMMENT 'tag:13',
  c4 BIGINT COMMENT 'tag:14',
  c5 BIGINT COMMENT 'tag:15',
  PRIMARY KEY (pk)
);
SQL
    dolt add test2
    dolt commit -m "another test commit"
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
}

@test "sql-remotes-server: generate a merge with a conflict with a remote branch" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "created table"
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "conflicting row"
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]]
    dolt conflicts resolve test --ours
    dolt add test
    dolt commit -m "Fixed conflicts"
    run dolt sql -q "select dolt_push('origin', 'master')"
    cd ../../
    dolt sql -q "select dolt_pull('test-remote')"
    run dolt log
    [[ "$output" =~ "Fixed conflicts" ]] || false
}

@test "sql-remotes-server: dolt_pull onto a dirty working set fails" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "created table"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "row to generate conflict"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt sql -q "insert into test values (0, 1, 1, 1, 1, 1)"
    run dolt sql -q "select dolt_pull('origin')"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot merge with uncommitted changes" ]] || false
}

@test "sql-remotes-server: force push to master" {
    skip "todo implement SQL push, pull, fetch"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt sql -q "select dolt_push('test-remote', 'master')"

    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..

    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt sql -q "select dolt_push('test-remote', 'master')"
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE other (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add other
    dolt commit -m "added other table"
    dolt fetch
    run dolt sql -q "dolt_push('origin', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
    dolt sql -q "select dolt_push('-f', 'master')"
    run dolt sql -q "select dolt_push('-f', 'origin', 'master')"
    [ "$status" -eq 0 ]
}


@test "sql-remotes-server: force fetch from master" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt remote -v

    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master

    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..

    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "test commit"
    # TODO dolt_push
    #dolt sql -q "select dolt_push('test-remote', 'master')"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt sql <<SQL
CREATE TABLE other (
  pk BIGINT NOT NULL COMMENT 'tag:10',
  c1 BIGINT COMMENT 'tag:11',
  c2 BIGINT COMMENT 'tag:12',
  c3 BIGINT COMMENT 'tag:13',
  c4 BIGINT COMMENT 'tag:14',
  c5 BIGINT COMMENT 'tag:15',
  PRIMARY KEY (pk)
);
SQL
    dolt add other
    dolt commit -m "added other table"
    dolt fetch
    # TODO dolt_push
    #dolt sql -q "select dolt_push('-f', 'origin', 'master')"
    dolt push -f origin master
    cd ../../
    run dolt fetch test-remote
    [ "$status" -ne 0 ]
    run dolt sql -q "select dolt_pull()"
    [ "$status" -ne 0 ]
    run dolt fetch -f test-remote
    [ "$status" -eq 0 ]
    dolt sql -q "select dolt_pull()"
}

@test "sql-remotes-server: not specifying a branch throws an error" {
    skip "todo implement SQL push, pull, fetch"
    run dolt sql -q "select dolt_push('-u', 'origin')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: --set-upstream requires <remote> and <refspec> params." ]] || false
}

@test "sql-remotes-server: pushing empty branch does not panic" {
    skip "todo implement SQL push, pull, fetch"
    run dolt sql -q "select dolt_push('origin, '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: ''" ]] || false
}

@test "sql-remotes-server: fetching unknown remotes should error" {
    skip "todo implement SQL push, pull, fetch"
    setup_ref_test
    cd ../../
    cd dolt-repo-clones/test-repo
    run dolt fetch remotes/dasdas
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "error: 'remotes/dasdas' is not a valid refspec." ]] || false
}

@test "sql-remotes-server: fetching added invalid remote correctly errors" {
    skip "todo implement SQL push, pull, fetch"
    setup_ref_test
    cd ../../
    cd dolt-repo-clones/test-repo
    dolt remote add myremote dolthub/fake

    run dolt fetch myremote
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "permission denied" ]] || false
}

@test "sql-remotes-server: fetching unknown remote ref errors accordingly" {
    skip "todo implement SQL push, pull, fetch"
    setup_ref_test
    cd ../../
    cd dolt-repo-clones/test-repo
    # Add a dummy remove to allow for fetching
    dolt remote add myremote dolthub/fake

    run dolt fetch dadasdfasdfa
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: dadasdfasdfa does not appear to be a dolt database" ]] || false
}
