#!/usr/bin/env bats

setup() { 
    if [ -z "$BATS_TMPDIR" ]; then
        export BATS_TMPDIR=$HOME/batstmp/
        mkdir $BATS_TMPDIR
    fi
    load $BATS_TEST_DIRNAME/helper/windows-compat.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir remotes-$$
    mkdir remotes-$$/test-org-empty
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    mkdir dolt-repo-$$
    cd dolt-repo-$$
    dolt init
    mkdir "dolt-repo-clones"
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo-$$
    pgrep remotesrv | xargs kill
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    pgrep remotesrv
}

@test "add a remote using dolt remote" {
    run dolt remote add test-remote http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt remote -v 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    run dolt remote add test-remote
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage:" ]] || false
}

@test "remove a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
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

@test "push and pull master branch from a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push test-remote master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -d "$BATS_TMPDIR/remotes-$$/test-org/test-repo" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say Already up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
}

@test "push and pull an unknown remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    run dolt push poop master
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt pull poop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false    
}

@test "push and pull non-master branch from remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    run dolt push test-remote test-branch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt pull test-remote
    [ "$status" -eq 0 ]
    skip "Should say up to date not fast forward"
    [[ "$output" = "up to date" ]] || false
}

@test "clone a remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [ "$output" = "cloning http://localhost:50051/test-org/test-repo" ]
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "clone an empty remote" {
    skip "Cloning an empty remote is busted"
    run dolt clone localhost:50051/test-org/empty
    [ "$status" -eq 0 ]
    cd empty
    run dolt status
    [ "$status" -eq 0 ]
}

@test "clone a non-existant remote" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    cd "dolt-repo-clones"
    run dolt clone foo/bar
    [ "$status" -eq 1 ]
    skip "Cloning a non-existant repository fails weirdly and leaves trash"
    [ "$output" = "fatal: repository 'foo/bar' does not exist" ]
    [[ ! "$output" =~ "permission denied" ]] || false
    [ ! -d bar ]
}

@test "clone a different branch than master" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt checkout -b test-branch
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote test-branch
    cd "dolt-repo-clones"
    run dolt clone -b test-branch http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [ "$output" = "cloning http://localhost:50051/test-org/test-repo" ]
    cd test-repo
    run dolt branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "master" ]] || false
    [[ "$output" =~ "test-branch" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "call a clone's remote something other than origin" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones"
    run dolt clone --remote test-remote http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    [ "$output" = "cloning http://localhost:50051/test-org/test-repo" ]
    cd test-repo
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
    run dolt remote -v 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test-remote" ]] || false
    [[ ! "$output" =~ "origin" ]] || false
}

@test "dolt fetch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    run dolt fetch test-remote
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/master
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt fetch poop refs/heads/master:refs/remotes/poop/master
    [ "$status" -eq 1 ]
    echo $output
    [[ "$output" =~ "unknown remote" ]] || false
    run dolt fetch test-remote refs/heads/master:refs/remotes/test-remote/poop
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch -v -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/test-remote/poop" ]] || false
} 

@test "dolt merge with origin/master syntax." {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    dolt fetch test-remote
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
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
    run dolt fetch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt merge origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
}

@test "dolt fetch and merge with remotes/origin/master syntax" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    skip "This remotes/origin/master syntax no longer works but works on git"
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    # This needs to say up-to-date like the skipped test above
    # [[ "$output" =~ "up to date" ]]
    dolt fetch origin master
    [ "$status" -eq 0 ]
    run dolt merge remotes/origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
}

@test "try to push a remote that is behind tip" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    run dolt push origin master
    [ "$status" -eq 0 ]
    [ "$output" = "Everything up-to-date" ] || false
    dolt fetch
    run dolt push origin master
    skip "dolt push when behind returns a 0 exit code now. should be 1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "tip of your current branch is behind" ]] || false
}

@test "generate a merge with no conflict with a remote branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test2
    dolt add test2
    dolt commit -m "another test commit"
    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
}

@test "generate a merge with a conflict with a remote branch" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "created table"
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd ..
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt add test
    dolt commit -m "row to generate conflict"
    dolt push test-remote master
    cd "dolt-repo-clones/test-repo"
    dolt table put-row test pk:0 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "conflicting row"
    run dolt pull origin
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]]
    dolt conflicts resolve test --ours
    dolt add test
    dolt commit -m "Fixed conflicts"
    run dolt push origin master
    cd ../../
    dolt pull test-remote
    run dolt log
    [[ "$output" =~ "Fixed conflicts" ]] || false
}

@test "clone sets your current branch appropriately" {
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"
    dolt checkout -b aaa
    dolt checkout -b zzz
    dolt push test-remote aaa
    dolt push test-remote zzz
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo
    cd test-repo

    # master hasn't been pushed so expect zzz to be the current branch and the string master should not be present
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "aaa" ]] || false    
    [[ "$output" =~ "* zzz" ]] || false
    [[ ! "$output" =~ "master" ]] || false
    cd ../..
    dolt push test-remote master
    cd "dolt-repo-clones"
    dolt clone http://localhost:50051/test-org/test-repo test-repo2
    cd test-repo2

    # master pushed so it should be the current branch.
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "aaa" ]] || false    
    [[ "$output" =~ "zzz" ]] || false
    [[ "$output" =~ "* master" ]] || false
}

@test "file based remotes" {
    # seed with some data
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"

    # push to a file based remote
    mkdir remotedir
    dolt remote add origin file://remotedir
    dolt push origin master

    # clone from a directory
    cd dolt-repo-clones
    dolt clone file://../remotedir test-repo
    cd test-repo

    # make modifications
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:1
    dolt add test
    dolt commit -m "put row"

    # push back to the other directory
    dolt push origin master
    run dolt branch --list master -v
    master_state1=$output

    # check that the remote master was updated
    cd ../..
    dolt pull
    run dolt branch --list master -v
    [[ "$output" = "$master_state1" ]] || false
}

@test "multiple remotes" {
    # seed with some data
    dolt table create -s=`nativepath $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema` test
    dolt add test
    dolt commit -m "test commit"

    # create the remote data storage directories
    mkdir remote1
    mkdir remote2

    # push to a file based remote
    dolt remote add remote2 file://remote2
    dolt push remote2 master

    # fetch fail for unspecified remote
    run dolt fetch
    [ "$status" -eq 1 ]

    # succeed when specifying a remote
    dolt fetch remote2

    #add origin push and fetch
    dolt remote add origin file://remote1
    dolt push master:notmaster

    #fetch should now work without a specified remote because origin exists
    dolt fetch

    # fetch master into some garbage tracking branches
    dolt fetch refs/heads/notmaster:refs/remotes/anything/master
    dolt fetch remote2 refs/heads/master:refs/remotes/something/master

    run dolt branch -a
    [[ "$output" =~ "remotes/anything/master" ]] || false
    [[ "$output" =~ "remotes/something/master" ]] || false
}