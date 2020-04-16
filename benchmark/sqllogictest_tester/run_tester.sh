#!/bin/bash

set -eo pipefail

function fail() {
    1>&2 echo "$@"
    exit 1
}

base_dir=$(cd ../../ && pwd)
sqllogictest_checkout="$base_dir/sqllogictest"
logictest="$base_dir/go/libraries/doltcore/sqle/logictest"
logictest_main="$logictest"/main
old_path=$PATH

if [[ "$#" -ne 1 ]]; then
    fail Usage: ./run_regressions.sh ENV_VARIABLES_FILE
fi

source "$1"
if [ -z "$DOLT_ROOT_PATH" ]; then fail Must supply DOLT_ROOT_PATH; fi
if [ -z "$DOLT_CONFIG_PATH" ]; then fail Must supply DOLT_CONFIG_PATH; fi
if [ -z "$DOLT_GLOBAL_CONFIG" ]; then fail Must supply DOLT_GLOBAL_CONFIG; fi
if [ -z "$CREDSDIR" ]; then fail Must supply CREDSDIR; fi
if [ -z "$DOLT_CREDS" ]; then fail Must supply DOLT_CREDS; fi
if [ -z "$CREDS_HASH" ]; then fail Must supply CREDS_HASH; fi
if [ -z "$TMP_TESTING_DIR" ]; then fail Must supply TMP_TESTING_DIR; fi
if [ -z "$TMP_CSV_DIR" ]; then fail Must supply TMP_CSV_DIR; fi
if [ -z "$TEST_FILE_DIR_LIST" ]; then fail Must supply TEST_FILE_DIR_LIST; fi
if [ -z "$COMMITS_TO_TEST" ]; then fail Must supply COMMITS_TO_TEST; fi

function setup() {
    rm -rf "$CREDSDIR"
    mkdir -p "$CREDSDIR"
    cat "$DOLT_CREDS" > "$CREDSDIR"/"$CREDS_HASH".jwk
    echo "$DOLT_GLOBAL_CONFIG" > "$DOLT_CONFIG_PATH"/config_global.json
    dolt config --global --add user.creds "$CREDS_HASH"
    dolt config --global --add metrics.disabled true
    dolt version
    rm -rf temp
    mkdir temp
}

function append_to_file_list() {
  if [ -z "$file_list" ]; then
    file_list="$1";
  else
    file_list="$file_list, $1"
  fi

  echo "Updated file_list:"
  echo "$file_list"
}

function setup_testing_dir() {
    rm -rf "$TMP_TESTING_DIR"
    mkdir -p "$TMP_TESTING_DIR"

    echo "Copying test files from sqllogictests to a temporary testing directory..."

    IFS=', ' read -r -a test_list <<< "$TEST_FILE_DIR_LIST"
    for fd in "${test_list[@]}"
    do
         cp -r "$sqllogictest_checkout"/test/"$fd" "$TMP_TESTING_DIR"/"$fd"
         append_to_file_list "$fd"
    done

    echo "Files/Directories that will be tested:"
    find "$TMP_TESTING_DIR"
}

function setup_query_db() {
    rm -f query_db
    touch query_db
    sqlite3 query_db < "$logictest"/regressions.sql
}

function with_dolt_commit() {
  local commit_hash="$1"
    (
      cd "$base_dir"/tempDolt/go
      git checkout master
      git checkout -b "temp-$commit_hash" "$commit_hash"
      git log -n 1
      if ! [ -x "$base_dir"/.ci_bin/"$commit_hash"/dolt ]; then
          if ! [ -d "$base_dir"/.ci_bin ]; then
            mkdir -p "$base_dir"/.ci_bin/"$commit_hash"
          fi
          echo Installing to .cibin, current wd:
          go get -mod=readonly ./...
          go build -mod=readonly -o "$base_dir"/.ci_bin/"$commit_hash"/dolt ./cmd/dolt/.
      fi
    )

    echo "Finished installing dolt from $commit_hash:"
    export PATH="$base_dir/.ci_bin/$commit_hash":$old_path
    dolt version
}

function with_dolt_checkout() {
    (
      cd ../../go
      if ! [ -x ../.ci_bin/dolt ]; then
          if ! [ -d ../.ci_bin ]; then
            mkdir -p ../.ci_bin
          fi
          go get -mod=readonly ./...
          go build -mod=readonly -o ../.ci_bin/dolt ./cmd/dolt/.
      fi
    )
    echo "Finished installing dolt from checkout:"
    export PATH=`pwd`"/../../.ci_bin":$old_path
    dolt version
}

function import_parsed() {
    local parsed="$1"
    local commit_hash="$2"
    dolt checkout regressions
    dolt checkout -b "temp-$commit_hash"
    dolt table import -u nightly_dolt_results "$parsed"

    dolt sql -r csv -q "\
    select * from nightly_dolt_results;"\
    > "$TMP_CSV_DIR"/"$commit_hash"_results.csv
    ls "$TMP_CSV_DIR"
    cat "$TMP_CSV_DIR"/"$commit_hash"_results.csv
    dolt checkout master
}

function import_and_query_db() {
    local commit_hash="$1"
    local db_copy="query_db_$commit_hash"
    cp query_db "$db_copy"

    sqlite3 "$db_copy" <<SQL
.mode csv
.import "$TMP_CSV_DIR"/"$commit_hash"_results.csv nightly_dolt_results
.import "$TMP_CSV_DIR"/release_results.csv releases_dolt_results
SQL

  result_query_output=`sqlite3 $db_copy 'select * from release_committer_result_change'`

  result_regressions=`echo $result_query_output | sed '/^\s*$/d' | wc -l | tr -d '[:space:]'`

  if [ "$result_regressions" != 0 ]; then echo "Result regression found, $result_regressions != 0" && echo $result_query_output && exit 1; else echo "No result regressions found"; fi

}

function run_once() {
    local commit_hash="$1"
    local results=temp/"results-$commit_hash".log
    local parsed=temp/"parsed-$commit_hash".json

    with_dolt_commit "$commit_hash"

    rm -rf .dolt
    dolt init

    echo "Running tests and logging raw results"
    go run . run "$TMP_TESTING_DIR" > "$results"

    echo "Parsing $results and generating $parsed"
    go run . parse "$commit_hash" "$results" > "$parsed"

    ls -ltra .

    (with_dolt_checkout; cd dolt-sql-performance; import_parsed "$parsed" "$commit_hash")

    import_and_query_db "$commit_hash"
}

function run() {
    IFS=', ' read -r -a commit_list <<< "$COMMITS_TO_TEST"
    for c in "${commit_list[@]}"
    do
        run_once "$c"
    done
    rm -rf .dolt
}

function import_nightly() {
    dolt checkout nightly
    seq 1 $TEST_N_TIMES | while read test_num; do
        import_one_nightly "$test_num"
    done
    dolt sql -r csv -q "\
select version, test_file, line_num, avg(duration) as mean_duration, result from dolt_history_nightly_dolt_results where version=\"${DOLT_VERSION}\" group by test_file, line_num;\
" > nightly_mean.csv
    dolt table import -u nightly_dolt_mean_results nightly_mean.csv
    dolt add nightly_dolt_mean_results
    dolt commit -m "update dolt sql performance mean results ($DOLT_VERSION)"
    dolt push origin nightly

    dolt checkout regressions
    dolt merge nightly
    dolt add .
    dolt commit -m "merge nightly"
    dolt push origin regressions

    dolt checkout releases
    dolt sql -r csv -q "\
select * from releases_dolt_mean_results;\
" > releases_mean.csv
    rm -f regressions_db
    touch regressions_db
    sqlite3 regressions_db < ../"$logictest"/regressions.sql
    cp ../"$logictest"/import.sql .
    sqlite3 regressions_db < import.sql
    echo "Checking for test regressions..."

    duration_query_output=`sqlite3 regressions_db 'select * from releases_nightly_duration_change'`
    result_query_output=`sqlite3 regressions_db 'select * from releases_nightly_result_change'`

    duration_regressions=`echo $duration_query_output | sed '/^\s*$/d' | wc -l | tr -d '[:space:]'`
    result_regressions=`echo $result_query_output | sed '/^\s*$/d' | wc -l | tr -d '[:space:]'`

    if [ "$duration_regressions" != 0 ]; then echo "Duration regression found, $duration_regressions != 0" && echo $duration_query_output && exit 1; else echo "No duration regressions found"; fi
    if [ "$result_regressions" != 0 ]; then echo "Result regression found, $result_regressions != 0" && echo $result_query_output && exit 1; else echo "No result regressions found"; fi
}

function create_releases_csv() {
    ls "$TMP_CSV_DIR"
    dolt checkout regressions
    dolt sql -r csv -q "select * from releases_dolt_results where test_file in ($file_list);"
    dolt sql -r csv -q "\
    select * from releases_dolt_results where test_file in ($file_list);"\
    > "$TMP_CSV_DIR"/release_results.csv
    ls "$TMP_CSV_DIR"
    cat "$TMP_CSV_DIR"/release_results.csv
    dolt checkout master
}

rm -rf dolt-sql-performance
(with_dolt_checkout; dolt clone Liquidata/dolt-sql-performance)

(with_dolt_checkout; setup; setup_testing_dir; setup_query_db)

(with_dolt_checkout; cd dolt-sql-performance; create_releases_csv)

(cd "$logictest_main"; run)

# then I want to cd into that dsp
# checkout regresssions
# run a query that gets the results for releases_dolt_results
# but limited to only the test files that are specified in the TEST_LIST
# get those results as csv

# create a temporary test dir
# copy all files/dirs from TEST_LIST into this test dir
# setup a sqlite3 db with schema for releases and nightly -- setup_query_db

# run a loop for each commit in COMMIT_LIST
# use with_dolt_commit(COMMIT) which will checkout dolt repo at that commit and install it
# then if LOG_RESULTS_ONLY is true, gen results but do not write to a file, so raw results

# are output to Jenkins logs
# otherwise, gen results to a results file
# using dolt binary from master,
# parse the results file
# import the parsed results into nightly_dolt_results
# get that data as a csv

# cp the empty sqlite3 db
# import release csv and commit(nightly) csv into sqlite3
# run query to see if regressions has occured between release and commit
# if it has not,
# continue loop
# if it has, output results of query to Jenkins log, fail the job




# rm -rf dolt-sql-performance
# (with_dolt_checkout; dolt clone Liquidata/dolt-sql-performance)

# (with_dolt_checkout; cd "$logictest_main"; setup)

#if [[ "$FAIL_ON_EXISTING_VERSION" == true ]]; then
#  (with_dolt_checkout; cd dolt-sql-performance; check_version_exists)
#fi

#if [[ "$JOB_TYPE" == "release" ]]; then
#   (with_dolt_release; cd "$logictest_main"; run)
#else
#   (with_dolt_checkout; cd "$logictest_main"; run)
#fi

#if [[ "$JOB_TYPE" == "nightly" ]]; then
#  (with_dolt_checkout; cd dolt-sql-performance; import_nightly);
#elif [ "$JOB_TYPE" == "release" ]; then
#  (with_dolt_checkout; cd dolt-sql-performance; import_releases)
#else fail Unknown JOB_TYPE specified;
#fi
