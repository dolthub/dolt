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

readTests="('oltp_read_only', 'oltp_point_select', 'select_random_points', 'select_random_ranges', 'covering_index_scan', 'index_scan', 'table_scan', 'groupby_scan', 'index_join_scan', 'types_table_scan', 'index_join')"
medianLatencyChangeReadsQuery="select f.test_name as read_tests, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, round(100 * ((avg(f.latency_percentile) - avg(t.latency_percentile)) / (avg(f.latency_percentile) + .0000001)), 2) as percent_change from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $readTests group by f.test_name;"

writeTests="('oltp_read_write', 'oltp_update_index', 'oltp_update_non_index', 'oltp_insert', 'oltp_write_only', 'oltp_delete_insert', 'types_delete_insert')"
medianLatencyChangeWritesQuery="select f.test_name as write_tests, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, round(100 * ((avg(f.latency_percentile) - avg(t.latency_percentile)) / (avg(f.latency_percentile) + .0000001)), 2) as percentage_change from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $writeTests group by f.test_name;"

tpccLatencyQuery="select f.test_name as test_name, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_p95, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_p95, round(100 * ((avg(f.latency_percentile) - avg(t.latency_percentile)) / (avg(f.latency_percentile) + .0000001)), 2) as percentage_change from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name LIKE '$tpccRegex' group by f.test_name;"
tpccTpsQuery="select f.test_name as test_name, f.server_name, f.server_version, avg(f.sql_transactions_per_second) as tps, t.test_name as test_name, t.server_name, t.server_version, avg(t.sql_transactions_per_second) as tps, round(100 * ((avg(f.latency_percentile) - avg(t.latency_percentile)) / (avg(f.latency_percentile) + .0000001)), 2) as percentage_change from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name LIKE 'tpcc%' group by f.test_name;"

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
              "--schema=/app/schema.sql",
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
              "--sysbenchQueries='"$medianLatencyChangeReadsQuery"'",
              "--sysbenchQueries='"$medianLatencyChangeWritesQuery"'",
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
