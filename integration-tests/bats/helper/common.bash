load helper/windows-compat

if [ -z "$BATS_TMPDIR" ]; then
    export BATS_TMPDIR=$HOME/batstmp/
    mkdir $BATS_TMPDIR
fi

nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
batshelper() { echo `nativebatsdir helper/$1`; }

stash_current_dolt_user() {
    export STASHED_DOLT_USER_NAME=`dolt config --global --get user.name`
    export STASHED_DOLT_USER_EMAIL=`dolt config --global --get user.email`
}

restore_stashed_dolt_user() {
    dolt config --global --add user.name "$STASHED_DOLT_USER_NAME"
    dolt config --global --add user.email "$STASHED_DOLT_USER_EMAIL"
    unset STASHED_DOLT_USER_NAME STASHED_DOLT_USER_EMAIL
}

set_dolt_user() {
    dolt config --global --add user.name "$1" > /dev/null 2>&1
    dolt config --global --add user.email "$2" > /dev/null 2>&1
}

unset_dolt_user() {
  dolt config --global --unset user.name
  dolt config --global --unset user.email
}

current_dolt_user_name() {
    dolt config --global --get user.name
}

current_dolt_user_email() {
    dolt config --global --get user.email
}

setup_no_dolt_init() {
    export PATH=$PATH:~/go/bin
    cd $BATS_TMPDIR
    
    # remove directory if exists
    # reruns recycle pids
    rm -rf "dolt-repo-$$"

    # Append the directory name with the pid of the calling process so
    # multiple tests can be run in parallel on the same machine
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
}

assert_feature_version() {
    run dolt version --feature
    # Tests that don't end in a valid dolt dir will fail the above
    # command, don't check its output in that case
    if [ "$status" -eq 0 ]; then
        [[ "$output" =~ "feature version: 3" ]] || exit 1
    else
      # Clear status to avoid BATS failing if this is the last run command
      status=0
    fi
}

skip_nbf_dolt() {
  if [ ! "$DOLT_DEFAULT_BIN_FORMAT" = "__LD_1__" ] &&
    [ ! "$DOLT_DEFAULT_BIN_FORMAT" = "__DOLT_DEV__" ];
  then
    skip "skipping test for nomsBinFormat __DOLT__"
  fi
}

skip_nbf_dolt_dev() {
  if [ "$DOLT_DEFAULT_BIN_FORMAT" = "__DOLT_DEV__" ]; then
    skip "skipping test for nomsBinFormat __DOLT_DEV__"
  fi
}

setup_common() {
    setup_no_dolt_init
    dolt init
}

teardown_common() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

log_status_eq() {
    if ! [ "$status" -eq $1 ]; then
        echo "status: expected $1, received $status"
        printf "output:\n$output"
        exit 1
    fi
}

log_output_has() {
    if ! [[ "$output" =~ $1 ]]; then
        echo "output did not have $1"
        printf "output:\n$output"
        exit 1
    fi
}

parquet() {
  if [ -z "$PARQUET_RUNTIME_JAR" ]; then
    echo "must supply PARQUET_RUNTIME_JAR to use parquet cli"
    exit 1
  else
    out=$(hadoop jar "$PARQUET_RUNTIME_JAR" org.apache.parquet.cli.Main "$@")
    echo "$out"
  fi
}

nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-$$ /p
dolt config --global --add metrics.disabled true > /dev/null 2>&1
set_dolt_user "Bats Tests" "bats@email.fake" 
