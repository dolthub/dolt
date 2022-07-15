#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

REMOTE=http://localhost:50051/test-org/test-repo

remotesrv_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_common
    cd $BATS_TMPDIR
    mkdir remotes-$$
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    remotesrv_pid=$!
    cd dolt-repo-$$
    dolt remote add test-remote $REMOTE
    dolt push test-remote main
    export DOLT_HEAD_COMMIT=`get_head_commit`
}

teardown() {
    teardown_common
    rm -rf $BATS_TMPDIR/git-repo-$$
    kill $remotesrv_pid
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "git-dolt: install sets up a smudge filter in the current git repository" {
    init_git_repo

    run git dolt install
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "Installed git-dolt smudge filter" ]] || false
    [[ "${lines[1]}" =~ "commit the changes to .gitattributes" ]] || false

    run cat .gitattributes
    [ "${lines[0]}" = "*.git-dolt filter=git-dolt" ]

    run cat .git/config
    len=${#lines[@]}
    [ "${lines[len-2]}" = "[filter \"git-dolt\"]" ]
    [[ "${lines[len-1]}" =~ "smudge = git-dolt-smudge" ]] || false
}

@test "git-dolt: install works in subdirectories of the git repository" {
    init_git_repo
    mkdir -p deeply/nested/directory
    pushd deeply/nested/directory
    run git dolt install
    [ "$status" -eq 0 ]

    popd
    run cat .gitattributes
    [ "${lines[0]}" = "*.git-dolt filter=git-dolt" ]

    run cat .git/config
    len=${#lines[@]}
    [ "${lines[len-2]}" = "[filter \"git-dolt\"]" ]
    [[ "${lines[len-1]}" =~ "smudge = git-dolt-smudge" ]] || false
}

@test "git-dolt: install fails with a helpful error when executed outside of a git repo" {
    run git dolt install
    [ "$status" -eq 1 ]
    [[ "$output" =~ "couldn't find a .git directory" ]] || false
}

@test "git-dolt: link takes a remote url (and an optional revspec and destination directory), clones the repo, and outputs a pointer file" {
    init_git_repo
    run git dolt link $REMOTE
    [ "$status" -eq 0 ]
    # Ensure it reports the resolved revision
    [[ "$output" =~ "revision $DOLT_HEAD_COMMIT" ]] || false
    # Ensure it reports the pointer filename
    [[ "$output" =~ "test-repo.git-dolt" ]] || false
    # Ensure it reports the addition to .gitignore
    [[ "$output" =~ "test-repo added to .gitignore" ]] || false
    [ -d test-repo ]

    run cat test-repo.git-dolt
    [[ "${lines[0]}" =~ ^version\ [0-9]+\.[0-9]+\.[0-9]+$ ]] || false
    [ "${lines[1]}" = "remote $REMOTE" ]
    [ "${lines[2]}" = "revision $DOLT_HEAD_COMMIT" ]

    run cat .gitignore
    [[ "${lines[0]}" =~ "test-repo" ]] || false
}

@test "git-dolt: smudge filter automatically clones dolt repositories referenced in checked out git-dolt pointer files" {
    init_git_repo
    git dolt install
    git dolt link $REMOTE
    git add .
    git commit -m "set up git-dolt integration"
    rm -rf test-repo test-repo.git-dolt

    run git checkout -- test-repo.git-dolt
    [[ "$output" =~ "Found git-dolt pointer file" ]] || false
    [[ "$output" =~ "Cloning remote $REMOTE" ]] || false
    [ -d test-repo ]

    cd test-repo
    [ `get_head_commit` = "$DOLT_HEAD_COMMIT" ]
}

@test "git-dolt: fetch takes the name of a git-dolt pointer file and clones the repo to the specified revision if it doesn't exist" {
    init_git_repo
    create_test_pointer

    run git dolt fetch test-repo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dolt repository cloned from remote $REMOTE to directory test-repo at revision $DOLT_HEAD_COMMIT" ]] || false
    [ -d test-repo ]

    cd test-repo
    [ `get_head_commit` = "$DOLT_HEAD_COMMIT" ]
}

@test "git-dolt: update updates the specified pointer file to the specified revision" {
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
    export NEW_DOLT_HEAD_COMMIT=`get_head_commit`

    init_git_repo
    create_test_pointer
    run git dolt update test-repo $NEW_DOLT_HEAD_COMMIT

    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "Updated pointer file test-repo.git-dolt to revision $NEW_DOLT_HEAD_COMMIT. You should git commit this change." ]

    run cat test-repo.git-dolt
    [[ "${lines[0]}" =~ ^version\ [0-9]+\.[0-9]+\.[0-9]+$ ]] || false
    [ "${lines[1]}" = "remote $REMOTE" ]
    [ "${lines[2]}" = "revision $NEW_DOLT_HEAD_COMMIT" ]
}

@test "git-dolt: fails helpfully when dolt is not installed" {
    mkdir TMP_PATH
    pushd TMP_PATH
    cp `which git` ./git
    cp `which git-dolt` ./git-dolt
    if [ $IS_WINDOWS = true ]; then
        ORIGINAL_PATH=$PATH
        export PATH=""
        export WSLENV=PATH
        run git dolt
        export PATH=$ORIGINAL_PATH
    else
        PATH=`pwd` run git dolt
    fi
    popd
    rm -rf TMP_PATH
    [ "$status" -eq 1 ]
    [[ "$output" =~ "It looks like Dolt is not installed on your system" ]] || false
}

@test "git-dolt: shows usage on unknown commands" {
    run git dolt nonsense
    [[ "$output" =~ Usage ]] || false
}

@test "git-dolt: prints usage information with no arguments" {
    run git dolt
    [[ "$output" =~ Usage ]] || false
}

init_git_repo() {
    mkdir ../git-repo-$$
    cd ../git-repo-$$
    git init
    git config user.email "foo@bar.com"
    git config user.name "Foo User"
}

create_test_pointer() {
    cat <<EOF > test-repo.git-dolt
version 0.0.0
remote $REMOTE
revision $DOLT_HEAD_COMMIT
EOF
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}
