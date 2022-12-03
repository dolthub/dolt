#!/bin/sh

set -e

if [ "$#" -lt 9 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <fromServer> <fromVersion> <toServer> <toVersion> <timePrefix> <actorPrefix> <format> <issueNumber> <initBigRepo> <nomsBinFormat> <withTpcc>"
    exit 1
fi

jobName="$1"
fromServer="$2"
fromVersion="$3"
toServer="$4"
toVersion="$5"
timePrefix="$6"
actorPrefix="$7"
format="$8"
issueNumber="$9"
initBigRepo="${10}"
nomsBinFormat="${11}"
withTpcc="${12}"
withSysbench="${13}"
tpccRegex="tpcc%"

if [ -n "$initBigRepo" ]; then
  initBigRepo="\"--init-big-repo=$initBigRepo\","
fi

if [ -n "$nomsBinFormat" ]; then
  nomsBinFormat="\"--noms-bin-format=$nomsBinFormat\","
fi

if [ -n "$withTpcc" ]; then
  withTpcc="\"--withTpcc=$withTpcc\","
fi

query1Tests="('oltp_read_only', 'oltp_point_select', 'select_random_points', 'select_random_ranges', 'covering_index_scan', 'index_scan', 'table_scan', 'groupby_scan', 'index_join_scan', 'types_table_scan', 'index_join')"
query2Tests="('oltp_read_write', 'oltp_update_index', 'oltp_update_non_index', 'oltp_insert', 'bulk_insert', 'oltp_write_only', 'oltp_delete_insert', 'types_delete_insert')"

query1Name="read_tests"
query2Name="write_tests"

latencyQuery1="select f.test_name as $query1Name, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, case when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) < -0.1 then 1 when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) > 0.1 then -1 else 0 end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $query1Tests group by f.test_name;"
latencyQuery2="select f.test_name as $query2Name, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, case when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) < -0.1 then 1 when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) > 0.1 then -1 else 0 end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $query2Tests group by f.test_name;"

if [ -n "$withSysbench" ]; then
    withSystab="\"--withSystab=$withSystab\","
    query1Name="sys_tab"
    query2Name="sys_tab_dummy"
    #query1Tests="('dolt_diff_table_commit_filter', 'dolt_diffs_commit_filter', 'dolt_history_commit_filter', 'dolt_log_commit_filter', 'dolt_commits_commit_filter', 'dolt_commit_ancestors_commit_filter', 'dolt_diff_log_join_on_commit')"
    #query2Tests="('dolt_diff_table_commit_filter_dummy', 'dolt_diffs_commit_filter_dummy', 'dolt_history_commit_filter_dummy', 'dolt_log_commit_filter_dummy', 'dolt_commits_commit_filter_dummy', 'dolt_commit_ancestors_commit_filter_dummy', 'dolt_diff_log_join_on_commit_dummy')"
    query1Tests="('gen/dolt_diff_table_commit_filter.gen.lua')"
    query2Tests="('gen/dolt_diff_table_commit_filter_dummy.gen.lua')"
    latencyQuery1="
Select name, dolt_multiple from (
  Select
    trim(TRAILING '.gen.lua' FROM trim(LEADING 'gen/' FROM test_name)) as name,
    round(latency_percentile / lead(latency_percentile) over w, 2) as dolt_multiple,
    row_number() over w as rn
  From from_results
  Having mod(rn,2) = 1
  Window w as (order by test_name ROWS between 1 preceding and 1 following)
)a;"
    latencyQuery2=
fi

tpccLatencyQuery="select f.test_name as test_name, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, case when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) < -0.25 then 1 when ((avg(t.latency_percentile) - avg(f.latency_percentile)) / (avg(f.latency_percentile) + .0000001)) > 0.25 then -1 else 0 end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name LIKE '$tpccRegex' group by f.test_name;"
tpccTpsQuery="select f.test_name as test_name, f.server_name, f.server_version, avg(f.sql_transactions_per_second) as tps, t.test_name as test_name, t.server_name, t.server_version, avg(t.sql_transactions_per_second) as tps, case when ((avg(t.sql_transactions_per_second) - avg(f.sql_transactions_per_second)) / (avg(f.sql_transactions_per_second) + .0000001)) < -0.5 then 1 when ((avg(t.sql_transactions_per_second) - avg(f.sql_transactions_per_second)) / (avg(f.sql_transactions_per_second) + .0000001)) > 0.5 then -1 else 0 end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name LIKE 'tpcc%' group by f.test_name;"

echo '
{
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "'$jobName'",
    "namespace": "performance-benchmarking"
  },
  "spec": {
    "backoffLimit": 1,
    "template": {
      "metadata": {
        "annotations": {
          "alert_recipients": "'$ACTOR_EMAIL'"
        },
        "labels": {
          "k8s-liquidata-inc-monitored-job": "created-by-static-config"
        }
      },
      "spec": {
        "serviceAccountName": "performance-benchmarking",
        "containers": [
          {
            "name": "performance-benchmarking",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/performance-benchmarking:latest",
            "resources": {
              "limits": {
                "cpu": "7000m"
              }
            },
            "env": [
              { "name": "GOMAXPROCS", "value": "7" },
              { "name": "ACTOR", "value": "'$ACTOR'" },
              { "name": "ACTOR_EMAIL", "value": "'$ACTOR_EMAIL'" },
              { "name": "REPO_ACCESS_TOKEN", "value": "'$REPO_ACCESS_TOKEN'" }
            ],
            "imagePullPolicy": "Always",
            "args": [
              "--schema=/schema.sql",
              "--useDoltHubLuaScriptsRepo",
              "--output='$format'",
              "--from-server='$fromServer'",
              "--from-version='$fromVersion'",
              "--to-server='$toServer'",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--issue-number='$issueNumber'",
              "--results-dir='$timePrefix'",
              "--results-prefix='$actorPrefix'",
              '"$withTpcc"'
              '"$initBigRepo"'
              '"$nomsBinFormat"'
              "--sysbenchQueries='"$latencyQuery1"'",
              "--sysbenchQueries='"$latencyQuery2"'",
              "--tpccQueries='"$tpccLatencyQuery"'",
              "--tpccQueries='"$tpccTpsQuery"'"
            ]
          }
        ],
        "restartPolicy": "Never",
        "nodeSelector": {
          "performance-benchmarking-worker": "true"
        },
        "tolerations": [
          {
              "effect": "NoSchedule",
              "key": "dedicated",
              "operator": "Equal",
              "value": "performance-benchmarking-worker"
          }
        ]
      }
    }
  }
}
'
