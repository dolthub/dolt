#!/bin/bash

set -eo pipefail

function fail() {
    1>&2 echo "$@"
    exit 1
}

base_dir=$(cd ../../ && pwd)
dsp_dir=$(pwd)
log_dir="$dsp_dir"/tempLogs
sqllogictest_checkout="$base_dir/sqllogictest"
logictest="$base_dir/tempDolt/go/libraries/doltcore/sqle/logictest"
logictest_main="$logictest"/main
schema_dir="$base_dir/go/libraries/doltcore/sqle/logictest"
old_path=$PATH

if [[ "$#" -ne 1 ]]; then
    fail Usage: ./run_regressions.sh ENV_VARIABLES_FILE
fi

source "$1"
if [ -z "$DOLT_ROOT_PATH" ]; then fail Must supply DOLT_ROOT_PATH; fi
if [ -z "$DOLT_CONFIG_PATH" ]; then fail Must supply DOLT_CONFIG_PATH; fi
if [ -z "$CREDSDIR" ]; then fail Must supply CREDSDIR; fi
if [ -z "$DOLT_CREDS" ]; then fail Must supply DOLT_CREDS; fi
if [ -z "$CREDS_HASH" ]; then fail Must supply CREDS_HASH; fi
if [ -z "$TMP_TESTING_DIR" ]; then fail Must supply TMP_TESTING_DIR; fi
if [ -z "$TMP_CSV_DIR" ]; then fail Must supply TMP_CSV_DIR; fi
if [ -z "$TEST_FILE_DIR_LIST" ]; then fail Must supply TEST_FILE_DIR_LIST; fi
if [ -z "$COMMITS_TO_TEST" ]; then fail Must supply COMMITS_TO_TEST; fi
if [ -z "$DOLT_BRANCH" ]; then fail Must supply DOLT_BRANCH; fi
if [ -z "$TEST_N_TIMES" ]; then fail Must supply TEST_N_TIMES; fi

[[ "$TEST_N_TIMES" =~ ^[0-9]+$ ]] || fail TEST_N_TIMES must be a number

function setup() {
    rm -rf "$CREDSDIR"
    mkdir -p "$CREDSDIR"
    dolt config --global --add metrics.disabled true
    dolt creds import "$DOLT_CREDS"
    dolt version
}

function setup_testing_dir() {
    rm -rf "$TMP_TESTING_DIR"
    mkdir -p "$TMP_TESTING_DIR"

    IFS=', ' read -r -a test_list <<< "$TEST_FILE_DIR_LIST"
    for fd in "${test_list[@]}"
    do
         dir=$(dirname "$fd")
         mkdir -p "$TMP_TESTING_DIR"/"$dir"
         cp -r "$sqllogictest_checkout"/test/"$fd" "$TMP_TESTING_DIR"/"$fd"
    done
}

function checkout_branch_if_exists() {
  local cmd="$1"
  local branch_name="$2"
  local hash="$3"

  if [ `"$cmd" branch --list "$branch_name" | wc -l` -eq 1 ]; then
    "$cmd" checkout "$branch_name"
  else
    if [ -z "$hash" ]; then
      "$cmd" checkout -b "$branch_name";
    else
      "$cmd" checkout -b "$branch_name" "$hash";
    fi
  fi
}

function with_dolt_commit() {
  local commit_hash="$1"
    (
      cd "$base_dir"/tempDolt/go
      git checkout "$commit_hash"

      checkout_branch_if_exists "git" "temp-$commit_hash" "$commit_hash"

      git log -n 1

      if ! [ -x "$base_dir"/.ci_bin/"$commit_hash"/dolt ]; then
          if ! [ -d "$base_dir"/.ci_bin ]; then
            mkdir -p "$base_dir"/.ci_bin/"$commit_hash"
          fi

          go build -mod=readonly -o "$base_dir"/.ci_bin/"$commit_hash"/dolt ./cmd/dolt/.
      fi
    )

    echo "Finished installing dolt from $commit_hash:"
    export PATH="$base_dir/.ci_bin/$commit_hash":$old_path
    dolt version
}

function with_dolt_checkout() {
    (
      cd "$base_dir"/go
      if ! [ -x "$base_dir"/.ci_bin/checkout/dolt ]; then
          if ! [ -d "$base_dir"/.ci_bin ]; then
            mkdir -p "$base_dir"/.ci_bin/checkout
          fi
          go get -mod=readonly ./...
          go build -mod=readonly -o "$base_dir"/.ci_bin/checkout/dolt ./cmd/dolt/.
      fi
    )
    echo "Finished installing dolt from checkout:"
    export PATH="$base_dir/.ci_bin/checkout":$old_path
    dolt version
}

function import_once() {
    local commit_hash="$1"
    local test_num="$2"
    local parsed="$log_dir/parsed-$commit_hash-$test_num".json

    dolt checkout "$DOLT_BRANCH"

    checkout_branch_if_exists "dolt" "temp-$commit_hash" ""

    dolt table import -u nightly_dolt_results "$parsed"

    dolt sql -r csv -q "\
    select * from nightly_dolt_results;"\
    > "$TMP_CSV_DIR"/"$commit_hash"_results.csv

    dolt add nightly_dolt_results
    dolt commit -m "add results for dolt at git commit $commit_hash ($test_num)"
    dolt checkout master
}

function create_mean_csv_once() {
    local commit_hash="$1"

    checkout_branch_if_exists "dolt" "temp-$commit_hash" ""

    dolt sql -r csv -q "\
    select version, test_file, line_num, avg(duration) as mean_duration, result from dolt_history_nightly_dolt_results where version=\"${commit_hash}\" group by test_file, line_num;"\
    > "$TMP_CSV_DIR"/"$commit_hash"_mean_results.csv

    dolt checkout master
}

function import_and_query_once() {
    rm -f query_db
    touch query_db
    sqlite3 query_db < "$schema_dir"/regressions.sql

    local commit_hash="$1"
    local release_csv="$TMP_CSV_DIR/release_results.csv"
    local release_mean_csv="$TMP_CSV_DIR/release_mean_results.csv"
    local commiter_csv="$TMP_CSV_DIR/${commit_hash}_results.csv"
    local commiter_mean_csv="$TMP_CSV_DIR/${commit_hash}_mean_results.csv"

    sqlite3 query_db <<SQL
.mode csv
.import $commiter_csv nightly_dolt_results
.import $release_csv releases_dolt_results
.import $commiter_mean_csv nigtly_dolt_mean_results
.import $release_mean_csv releases_dolt_mean_results
SQL

  result_query_output=`sqlite3 query_db 'select * from release_committer_result_change'`
  duration_query_output=`sqlite3 query_db 'select * from releases_nightly_duration_change'`

  result_regressions=`echo $result_query_output | sed '/^\s*$/d' | wc -l | tr -d '[:space:]'`
  duration_regressions=`echo $duration_query_output | sed '/^\s*$/d' | wc -l | tr -d '[:space:]'`

  if [ "$result_regressions" != 0 ]; then echo "Result regression found, $result_regressions != 0" && echo $result_query_output && exit 1; else echo "No result regression found"; fi
  if [ "$duration_regressions" != 0 ]; then echo "Duration regression found, $duration_regressions != 0" && echo $duration_query_output && exit 1; else echo "No duration regressions found"; fi
}

function run_once() {
    local commit_hash="$1"
    local test_num="$2"
    local results="$log_dir/results-$commit_hash-$test_num".log
    local parsed="$log_dir/parsed-$commit_hash-$test_num".json

    (
      with_dolt_commit "$commit_hash"

      rm -rf .dolt; dolt init

      echo "Running tests and creating $results"
      go run . run "$TMP_TESTING_DIR" > "$results"

      echo "Parsing $results and generating $parsed"
      go run . parse "$commit_hash" "$results" > "$parsed"
    )

    (with_dolt_checkout; cd "$dsp_dir"/dolt-sql-performance; import_once "$c" "$test_num")
}

function run() {
    rm -rf "$log_dir"
    mkdir "$log_dir"

    IFS=', ' read -r -a commit_list <<< "$COMMITS_TO_TEST"
    for c in "${commit_list[@]}"
    do
        seq 1 $TEST_N_TIMES | while read test_num; do
          run_once "$c" "$test_num"
        done

        (with_dolt_checkout; cd "$dsp_dir"/dolt-sql-performance; create_mean_csv_once "$c")

        (import_and_query_once "$c")
    done
}

append() {
  echo "$1""${1:+, }""'$2'"
}

function create_releases_csv() {
    test_files=$(find "$TMP_TESTING_DIR" | sed -n "s|^$TMP_TESTING_DIR/||p")

    SAVEIFS=$IFS
    IFS=$'\n'

    # do not wrap env var in quotes, so it gets split into array
    file_arr=($test_files)

    IFS=$SAVEIFS

    file_list=
    for (( i=0; i<${#file_arr[@]}; i++ ))
    do
       if [ "${file_arr[$i]: -5}" == ".test" ]; then
        file_list=`append "$file_list" "${file_arr[$i]}"`
       fi
    done

    dolt checkout "$DOLT_BRANCH"
    dolt sql -r csv -q "\
    select * from releases_dolt_results where test_file in ($file_list);"\
    > "$TMP_CSV_DIR"/release_results.csv

    dolt sql -r csv -q "\
    select * from releases_dolt_mean_results where test_file in ($file_list);"\
    > "$TMP_CSV_DIR"/release_mean_results.csv

    dolt checkout master
}

function update_fetch_specs() {
  local remote="$1"
  local branch="$2"
  repo_state=$(cat .dolt/repo_state.json)
  jq ".remotes.$remote.fetch_specs = [\"refs/heads/$branch:refs/remotes/origin/$branch\"]" <<< "$repo_state" > .dolt/repo_state.json
}

function fetch_repo() {
    dolt init
    dolt remote add origin "https://doltremoteapi.dolthub.com/Liquidata/dolt-sql-performance"
    update_fetch_specs "origin" "$DOLT_BRANCH"
    dolt fetch origin
}

(with_dolt_checkout; setup)

rm -rf dolt-sql-performance && mkdir dolt-sql-performance
(with_dolt_checkout; cd dolt-sql-performance; fetch_repo)

(with_dolt_checkout; setup_testing_dir)

(with_dolt_checkout; cd dolt-sql-performance; create_releases_csv)

(cd "$logictest_main"; run)
