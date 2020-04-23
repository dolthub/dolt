#!/bin/bash

set -eo pipefail

function fail() {
    1>&2 echo "$@"
    exit 1
}

logictest="../../go/libraries/doltcore/sqle/logictest"
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
if [ -z "$JOB_TYPE" ]; then fail Must supply JOB_TYPE; fi
if [ -z "$TEST_N_TIMES" ]; then fail Must supply TEST_N_TIMES; fi
if [ -z "$FAIL_ON_EXISTING_VERSION" ]; then fail Must supply FAIL_ON_EXISTING_VERSION; fi
if [ -z "$USE_PROD_REPO" ]; then fail Must supply USE_PROD_REPO; fi
if [ -z "$DEV_REMOTE_HOST" ]; then fail Must supply DEV_REMOTE_HOST; fi

if [[ -z "$DOLT_VERSION" && -z "$DOLT_RELEASE" ]]; then
  fail Must supply DOLT_VERSION;
elif [[ -z "$DOLT_VERSION" && -n "$DOLT_RELEASE" ]]; then
    DOLT_VERSION="$DOLT_RELEASE";
fi

[[ "$TEST_N_TIMES" =~ ^[0-9]+$ ]] || fail TEST_N_TIMES must be a number

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

function run_once() {
    test_num="$1"

    local results=temp/results"$test_num".log
    local parsed=temp/parsed"$test_num".json

    rm -rf .dolt
    dolt init
    echo "Running tests and generating $results"
    go run . run ../../../../../../sqllogictest/test > "$results"
    echo "Parsing $results and generating $parsed"
    go run . parse "$DOLT_VERSION" temp/results"$test_num".log > "$parsed"
}

function run() {
    seq 1 $TEST_N_TIMES | while read test_num; do
        run_once "$test_num"
    done
    rm -rf .dolt
}

function check_version_exists() {
    if [[ "$JOB_TYPE" == "nightly" ]]; then
      dolt checkout nightly
      table_prefix="nightly"
    elif [ "$JOB_TYPE" == "release" ]; then
        dolt checkout releases
        table_prefix="releases"
      else fail Unknown JOB_TYPE specified;
    fi

    previously_tested_version=$(dolt sql -r csv -q "select * from ${table_prefix}_dolt_results where version = '$DOLT_VERSION' limit 1;"| wc -l | tr -d '[:space:]')

    if [ "$previously_tested_version" != 1 ]; then
      echo "Results for dolt version $DOLT_VERSION already exist in Liquidata/dolt-sql-performance, $previously_tested_version != 1" && \
      echo $result_query_output && \
      exit 155;
    fi

    dolt checkout master
}

function branch_from_base() {
    local branch_name="$1"
    dolt checkout regressions

    dolt sql -r csv -q "select * from releases_dolt_results" > "$branch_name"_releases_results.csv
    dolt sql -r csv -q "select * from releases_dolt_mean_results" > "$branch_name"_releases_mean_results.csv

    dolt checkout regressions-tip-base
    dolt checkout -b "$branch_name"

    dolt table import -u releases_dolt_results "$branch_name"_releases_results.csv
    dolt add releases_dolt_results
    dolt commit -m "add release data from tip of regressions"

    dolt table import -u releases_dolt_mean_results "$branch_name"_releases_mean_results.csv
    dolt add releases_dolt_mean_results
    dolt commit -m "add release mean data from tip of regressions"

    dolt checkout master
}

function update_regressions_latest() {
  dolt checkout master
  exists=$(dolt branch --list regressions-tip-previous | sed '/^\s*$/d' | wc -l | tr -d '[:space:]')

  if [ "$exists" -eq 1 ]; then
    dolt branch -d -f regressions-tip-previous;
  fi

  exists=$(dolt branch --list regressions-tip-latest | sed '/^\s*$/d' | wc -l | tr -d '[:space:]')

  if [ "$exists" -eq 1 ]; then
    dolt checkout regressions-tip-latest

    dolt branch -m regressions-tip-latest regressions-tip-previous
    dolt push -f origin regressions-tip-previous

    branch_from_base "regressions-tip-latest"

    dolt checkout regressions-tip-latest
    dolt push -f origin regressions-tip-latest;
  else
    branch_from_base "regressions-tip-previous"
    branch_from_base "regressions-tip-latest"

    dolt checkout regressions-tip-previous
    dolt push -f origin regressions-tip-previous

    dolt checkout regressions-tip-latest
    dolt push regressions-tip-latest;
  fi
  dolt checkout master
}

function import_one_nightly() {
    test_num="$1"
    dolt table import -u nightly_dolt_results ../"$logictest_main"/temp/parsed"$test_num".json
    dolt add nightly_dolt_results
    dolt commit -m "update dolt sql performance results ($DOLT_VERSION) ($test_num)"
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

    update_regressions_latest

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

function with_dolt_release() {
    (
        cd ../../
        if ! [ -x ./.ci_bin/dolt_release/dolt ]; then
            if ! [ -d ./.ci_bin/dolt_release ]; then
              mkdir -p ./.ci_bin/dolt_release
            fi
            curl -A ld-jenkins-dolt-installer -fL "$DOLT_RELEASE_URL" > dolt.tar.gz
            tar zxf dolt.tar.gz
            install dolt-linux-amd64/bin/dolt ./.ci_bin/dolt_release/
        fi
    )
    echo "Finished installing dolt from release:"
    export PATH=`pwd`"/../../.ci_bin/dolt_release":$old_path
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

function import_one_releases() {
    test_num="$1"
    dolt table import -u releases_dolt_results ../"$logictest_main"/temp/parsed"$test_num".json
    dolt add releases_dolt_results
    dolt commit -m "update dolt sql performance results ($DOLT_VERSION) ($test_num)"
}

function import_releases() {
    dolt checkout releases
    seq 1 $TEST_N_TIMES | while read test_num; do
        import_one_releases "$test_num"
    done
    dolt sql -r csv -q "\
select version, test_file, line_num, avg(duration) as mean_duration, result from dolt_history_releases_dolt_results where version=\"${DOLT_VERSION}\" group by test_file, line_num;\
" > releases_mean.csv
    dolt table import -u releases_dolt_mean_results releases_mean.csv
    dolt add releases_dolt_mean_results
    dolt commit -m "update dolt sql performance mean results ($DOLT_VERSION)"
    dolt push origin releases

    dolt checkout regressions
    dolt merge releases
    dolt add .
    dolt commit -m "merge releases"
    dolt push origin regressions
}

rm -rf dolt-sql-performance

(
  with_dolt_checkout
  if [[ "$USE_PROD_REPO" == true ]]; then
    dolt clone Liquidata/dolt-sql-performance;
  else
    dolt clone "$DEV_REMOTE_HOST"/Liquidata/dolt-sql-performance
  fi

)

(with_dolt_checkout; cd "$logictest_main"; setup)

if [[ "$FAIL_ON_EXISTING_VERSION" == true ]]; then
  (with_dolt_checkout; cd dolt-sql-performance; check_version_exists)
fi

if [[ "$JOB_TYPE" == "release" ]]; then
   (with_dolt_release; cd "$logictest_main"; run)
else
   (with_dolt_checkout; cd "$logictest_main"; run)
fi

if [[ "$JOB_TYPE" == "nightly" ]]; then
  (with_dolt_checkout; cd dolt-sql-performance; import_nightly);
elif [ "$JOB_TYPE" == "release" ]; then
  (with_dolt_checkout; cd dolt-sql-performance; import_releases)
else fail Unknown JOB_TYPE specified;
fi
