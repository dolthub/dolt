#!/usr/bin/env bats

# Git remotes allow using git repositories as dolt remote backends
# These tests cover the `dolt remote init` command which initializes
# a git repository for use as a dolt remote.
#
# Note: Full push/pull/clone integration with git remotes requires
# additional work to integrate the GitFactory with dolt's remote
# operations. These tests focus on the initialization functionality.

load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cd $BATS_TMPDIR
    cd dolt-repo-$$
    mkdir "git-remotes"
}

teardown() {
    assert_feature_version
    teardown_common
}

# Helper function to create a bare git repository
create_bare_git_repo() {
    local name=$1
    local repo_path="git-remotes/${name}.git"
    
    # Debug output
    echo "DEBUG: Creating bare git repo: ${repo_path}"
    echo "DEBUG: Current directory: $(pwd)"
    echo "DEBUG: PATH=${PATH}"
    echo "DEBUG: which git: $(which git 2>&1)"
    echo "DEBUG: git --version: $(git --version 2>&1)"
    echo "DEBUG: /usr/bin/git exists: $(test -x /usr/bin/git && echo 'yes' || echo 'no')"
    
    # Try to create the repo
    if ! git init --bare "${repo_path}"; then
        echo "DEBUG: git init failed with exit code $?"
        return 1
    fi
    
    echo "DEBUG: Successfully created ${repo_path}"
    ls -la "${repo_path}"
}

@test "remotes-git: remote init creates dolt structure" {
    create_bare_git_repo "test-repo"
    
    # Initialize the git repo as a dolt remote
    run dolt remote init "git-remotes/test-repo.git"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized" ]] || false
    
    # Verify the ref was created
    cd git-remotes/test-repo.git
    run git show-ref
    [ "$status" -eq 0 ]
    [[ "$output" =~ "refs/dolt/data" ]] || false
}

@test "remotes-git: remote init is idempotent" {
    create_bare_git_repo "idempotent-repo"
    
    # First init
    run dolt remote init "git-remotes/idempotent-repo.git"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized" ]] || false
    
    # Second init should succeed and report already initialized
    run dolt remote init "git-remotes/idempotent-repo.git"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "already initialized" ]] || false
}

@test "remotes-git: remote init with custom ref" {
    create_bare_git_repo "custom-ref-repo"
    
    # Initialize with custom ref
    run dolt remote init --ref refs/dolt/custom "git-remotes/custom-ref-repo.git"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized" ]] || false
    
    # Verify the custom ref was created
    cd git-remotes/custom-ref-repo.git
    run git show-ref
    [ "$status" -eq 0 ]
    [[ "$output" =~ "refs/dolt/custom" ]] || false
}

@test "remotes-git: remote init fails on invalid URL" {
    # Try to init a non-.git path
    run dolt remote init "/tmp/not-a-git-repo"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid git remote URL" ]] || false
}

@test "remotes-git: remote init creates README in .dolt_remote" {
    create_bare_git_repo "readme-repo"
    
    # Initialize
    run dolt remote init "git-remotes/readme-repo.git"
    [ "$status" -eq 0 ]
    
    # Clone the repo and fetch the dolt ref
    cd git-remotes
    git clone readme-repo.git readme-check > /dev/null 2>&1
    cd readme-check
    git fetch origin refs/dolt/data:refs/dolt/data > /dev/null 2>&1
    git checkout FETCH_HEAD > /dev/null 2>&1
    
    # Verify README exists
    [ -f ".dolt_remote/README.md" ]
    
    # Verify data directory exists
    [ -d ".dolt_remote/data" ]
}

@test "remotes-git: remote init works with absolute path" {
    create_bare_git_repo "abs-path-repo"
    
    # Get absolute path
    abs_path="$PWD/git-remotes/abs-path-repo.git"
    
    # Initialize using absolute path
    run dolt remote init "$abs_path"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized" ]] || false
}

@test "remotes-git: remote init detects non-existent repo" {
    # Try to init a path that doesn't exist
    run dolt remote init "git-remotes/nonexistent.git"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "failed to open git repository" ]] || false
}

@test "remotes-git: push and clone with git remote" {
    create_bare_git_repo "push-clone-repo"
    
    # Initialize the git remote
    dolt remote init "git-remotes/push-clone-repo.git"
    
    # Create some data
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 VARCHAR(100),
  PRIMARY KEY (pk)
);
INSERT INTO test VALUES (1, 'hello'), (2, 'world');
SQL
    dolt add test
    dolt commit -m "initial commit"
    
    # Add the git remote and push
    dolt remote add origin "git-remotes/push-clone-repo.git"
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]
    
    # Clone to a new directory
    mkdir -p dolt-repo-clones
    cd dolt-repo-clones
    run dolt clone "git:///$(cd .. && pwd)/git-remotes/push-clone-repo.git" cloned-repo
    [ "$status" -eq 0 ]
    
    cd cloned-repo
    
    # Verify data
    run dolt sql -q "SELECT * FROM test ORDER BY pk"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello" ]] || false
    [[ "$output" =~ "world" ]] || false
}

@test "remotes-git: fetch and pull updates" {
    create_bare_git_repo "fetch-pull-repo"
    
    # Initialize and push initial data
    dolt remote init "git-remotes/fetch-pull-repo.git"
    
    dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY, val VARCHAR(50))"
    dolt sql -q "INSERT INTO data VALUES (1, 'initial')"
    dolt add data
    dolt commit -m "initial"
    
    dolt remote add origin "git-remotes/fetch-pull-repo.git"
    dolt push --set-upstream origin main
    
    # Clone
    mkdir -p dolt-repo-clones
    cd dolt-repo-clones
    dolt clone "git:///$(cd .. && pwd)/git-remotes/fetch-pull-repo.git" fetch-test
    cd ..
    
    # Make changes in original and push
    dolt sql -q "INSERT INTO data VALUES (2, 'second')"
    dolt add data
    dolt commit -m "added second row"
    dolt push origin main
    
    # Pull in clone
    cd dolt-repo-clones/fetch-test
    run dolt pull origin main
    [ "$status" -eq 0 ]
    
    # Verify
    run dolt sql -q "SELECT COUNT(*) as cnt FROM data"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}
