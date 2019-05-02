#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir remotes-$$
    remotesrv --http-port 1234 --dir $BATS_TMPDIR/remotes-$$ &>/dev/null 3>&- &
    mkdir dolt-repo-$$
    cd dolt-repo-$$
    dolt init
    mkdir "dolt-repo-clones"
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo-$$
    pkill -2 remotesrv
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    pgrep remotesrv
}

@test "add a remote using dolt remote" {
    run dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    run dolt remote add test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage:" ]] || false
}

@test "push and pull from a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt push poop master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say Already up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
    run dolt pull poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "rename a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote rename test-remote renamed-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "renamed-remote" ]] || false
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote rename poop test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "remove a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    run dolt remote remove test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test-remote" ]] || false
    run dolt remote remove poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
}

@test "clone a remote" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone localhost:50051/test-org/test-repo --insecure
    [ "$status" -eq 0 ]
    # [ "$output" = "cloning localhost:50051/test-org/test-repo" ]
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "dolt fetch" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    run dolt fetch test-remote master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch poop master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote poop" ]] || false
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt log
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test commit" ]] || false
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    run dolt fetch origin master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "try to push a remote that is behind tip" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt push origin master
    [ "$status" -eq 0 ]
    [ "$output" = "Everything up-to-date" ] || false
    dolt fetch origin master
    run dolt push origin master
    skip "dolt push when behind returns a 0 exit code now. should be 1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "generate a merge with no conflict with a remote branch" {
    dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone localhost:50051/test-org/test-repo --insecure
    cd ..
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test2
    dolt add test2
    dolt commit -m "another test commit"
    run dolt pull origin
    skip "This panics right now with a panic: runtime error: invalid memory address or nil pointer dereference"
    [ "$status" -eq 0 ]
}
