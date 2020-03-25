#!/bin/bash

set -eo pipefail

function fail() {
    1>&2 echo "$@"
    exit 1
}

logictest="go/libraries/doltcore/sqle/logictest"
logictest_main="$logictest"/main

if [[ "$#" -ne 1 ]]; then
    fail Usage: ./run_regressions.sh ENV_VARIABLES_FILE
fi

source "$1"
if [ -z "$DOLT_CONFIG_PATH" ]; then fail Must supply DOLT_CONFIG_PATH; fi
if [ -z "$DOLT_GLOBAL_CONFIG" ]; then fail Must supply DOLT_GLOBAL_CONFIG; fi
if [ -z "$CREDSDIR" ]; then fail Must supply CREDSDIR; fi
if [ -z "$DOLT_CREDS" ]; then fail Must supply DOLT_CREDS; fi
if [ -z "$CREDS_HASH" ]; then fail Must supply CREDS_HASH; fi
if [ -z "$DOLT_VERSION" ]; then fail Must supply DOLT_VERSION; fi

function setup() {
    echo "$DOLT_GLOBAL_CONFIG" > "$DOLT_CONFIG_PATH"/config_global.json
    rm -rf "$CREDSDIR"
    mkdir "$CREDSDIR"
    cat "$DOLT_CREDS" > "$CREDSDIR"/"$CREDS_HASH".jwk
    dolt config global --add user.creds "$CREDS_HASH"
    dolt config global --add metrics.disabled true
    rm -rf temp
    mkdir temp
}

function run_once() {
    test_num="$1"
    rm -rf .dolt
    dolt init
    go run . run ../../../../../../sqllogictest/test/select1.test > temp/results"$test_num".log
    go run . parse "$DOLT_VERSION" temp/results"$test_num".log > tmp/results"$test_num".json
}

function run() {
    seq 1 | while read test_num; do
        run_once "$test_num"
    done
    rm -rf .dolt
}

function import_one() {
    test_name="$1"
    dolt table import -u nightly_dolt_results ../"$logictest_main"/temp/parsed"$test_num".json
    dolt add nightly_dolt_results
    dolt commit -m "update dolt sql performance results ($DOLT_VERSION) ($test_num)"
}

function import() {
    dolt checkout nightly
    seq 1 | while read test_num; do
        import_one "$test_num"
    done
    dolt sql -r csv -q "\
select version, test_file, line_num, avg(duration) as mean_duration, result from dolt_history_nightly_dolt_results where version=\"${DOLT_VERSION}\" group by line_num;\
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
    duration_regressions=`sqlite3 regressions_db 'select * from releases_nightly_duration_change' | wc -l | tr -d '[:space:]'`
    result_regressions=`sqlite3 regressions_db 'select * from releases_nightly_result_change' | wc -l | tr -d '[:space:]'`
    if [ "$duration_regressions" != 0 ]; then exit 1; fi
    if [ "$result_regressions" != 0 ]; then exit 1; fi
}

(cd "$logictest_main" && setup && run)

rm -rf dolt-sql-performance
dolt clone Liquidata/dolt-sql-performance
(cd dolt-sql-performance && import)
