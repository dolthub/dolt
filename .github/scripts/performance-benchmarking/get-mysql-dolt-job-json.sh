#!/bin/sh

set -e

if [ "$#" -ne 8 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <fromServer> <fromVersion> <toServer> <toVersion> <timeprefix> <actorprefix> <format>"
    exit 1
fi

jobname="$1"
fromServer="$2"
fromVersion="$3"
toServer="$4"
toVersion="$5"
timeprefix="$6"
actorprefix="$7"
format="$8"

readTests="('oltp_read_only', 'oltp_point_select', 'select_random_points', 'select_random_ranges', 'covering_index_scan', 'index_scan', 'table_scan')"
medianLatencyMultiplierReadsQuery="select f.test_name as read_tests, f.server_name, f.server_version, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, t.server_name, t.server_version, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, case when ROUND(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001)) < 1.0 then 1.0 else ROUND(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001)) end as multiplier from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $readTests group by f.test_name;"
meanMultiplierReadsQuery="select round(avg(multipliers)) as reads_mean_multiplier from (select case when (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) < 1.0 then 1.0 else (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) end as multipliers from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $readTests group by f.test_name)"

writeTests="('oltp_read_write', 'oltp_update_index', 'oltp_update_non_index', 'oltp_insert', 'bulk_insert', 'oltp_write_only', 'oltp_delete')"
medianLatencyMultiplierWritesQuery="select f.test_name as write_tests, f.server_name, f.server_version, case when avg(f.latency_percentile) < 0.001 then 0.001 else avg(f.latency_percentile) end as from_latency_median, t.server_name, t.server_version, case when avg(t.latency_percentile) < 0.001 then 0.001 else avg(t.latency_percentile) end as to_latency_median, case when ROUND(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001)) < 1.0 then 1.0 else ROUND(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001)) end as multiplier from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $writeTests group by f.test_name;"
meanMultiplierWritesQuery="select round(avg(multipliers)) as writes_mean_multiplier from (select case when (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) < 1.0 then 1.0 else (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) end as multipliers from from_results as f join to_results as t on f.test_name = t.test_name where f.test_name in $writeTests group by f.test_name)"

meanMultiplierOverallQuery="select round(avg(multipliers)) as overall_mean_multiplier from (select case when (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) < 1.0 then 1.0 else (round(avg(t.latency_percentile) / (avg(f.latency_percentile) + .000001))) end as multipliers from from_results as f join to_results as t on f.test_name = t.test_name group by f.test_name)"

echo '
{
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "'$jobname'",
    "namespace": "performance-benchmarking"
  },
  "spec": {
    "backoffLimit": 1,
    "template": {
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
              {
                "name": "GOMAXPROCS",
                "value": "7"
              }
            ],
            "args": [
              "--schema=/schema.sql",
              "--script-dir=/scripts/lua",
              "--output='$format'",
              "--mysql-exec=/usr/sbin/mysqld",
              "--mysql-protocol=unix",
              "--from-server='$fromServer'",
              "--from-version='$fromVersion'",
              "--to-server='$toServer'",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "'"$medianLatencyMultiplierReadsQuery"'",
              "'"$meanMultiplierReadsQuery"'",
              "'"$medianLatencyMultiplierWritesQuery"'",
              "'"$meanMultiplierWritesQuery"'",
              "'"$meanMultiplierOverallQuery"'"
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
