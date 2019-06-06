#!/usr/bin/env bats

setup() { 
  if [ -z "$BATS_TMPDIR" ]; then
      export BATS_TMPDIR=$HOME/batstmp/
      mkdir $BATS_TMPDIR
  fi

  export PATH=$PATH:~/go/bin
  export NOMS_VERSION_NEXT=1
  cd $BATS_TMPDIR
  mkdir remotes-$$
  echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
  remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
  mkdir dolt-repo-$$
  cd dolt-repo-$$
  dolt init
  dolt remote add test-remote localhost:50051/test-org/test-repo --insecure
  dolt push test-remote master
}

teardown() {
  rm -rf $BATS_TMPDIR/{git,dolt}-repo-$$
  pkill -2 remotesrv
  rm -rf $BATS_TMPDIR/remotes-$$
}

@test "git dolt link takes a remote url (and an optional revspec and destination directory), clones the repo, and outputs a pointer file" {
  mkdir ../git-repo-$$
  cd ../git-repo-$$
  git init
  run git dolt link localhost:50051/test-org/test-repo
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" =~ "Success!" ]] || false
  [[ "${lines[1]}" =~ "Dolt repository cloned to test-repo" ]] || false
  [[ "${lines[2]}" =~ "Pointer file created at test-repo.git-dolt" ]] || false
  [[ "${lines[3]}" =~ "test-repo added to .gitignore" ]] || false
  [[ "${lines[4]}" =~ "You should git commit these results" ]] || false
  [ -d test-repo ]

  run cat test-repo.git-dolt
  [[ "${lines[0]}" =~ "version 0" ]] || false
  [[ "${lines[1]}" =~ "remote localhost:50051/test-org/test-repo" ]] || false
  [[ "${lines[2]}" =~ ^(revision [0-9a-v]{32})$ ]] || false

  run cat .gitignore
  [[ "${lines[0]}" =~ "test-repo" ]] || false
}

@test "git dolt fails helpfully when dolt is not installed" {
  mkdir TMP_PATH
  pushd TMP_PATH
  which git | xargs ln -sf
  which git-dolt | xargs ln -sf
  popd
  PATH=`pwd`/TMP_PATH run git dolt
  rm -rf TMP_PATH
  [ "$status" -eq 1 ]
  [[ "$output" =~ "It looks like Dolt is not installed on your system" ]]
}

@test "git dolt prints usage information with no arguments" {
  run git dolt
  [[ "$output" =~ Usage ]] || false
}