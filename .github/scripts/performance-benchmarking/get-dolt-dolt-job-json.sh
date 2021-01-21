#!/bin/sh

set -e

if [ "$#" -ne 8 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <fromServer> <fromVersion> <toServer> <toVersion> <timePrefix> <actorPrefix> <format>"
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

averageTimeChangeQuery="select f.test_name as test_name, ROUND(100 * (1.0 - ((AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) / (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001))))) as average_time_percent_change, case when (100 * (1.0 - ((AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) / (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001))))) < 0 then true else false end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name group by f.test_name;"

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
      "spec": {
        "serviceAccountName": "performance-benchmarking",
        "containers": [
          {
            "name": "performance-benchmarking",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/performance-benchmarking:latest",
            "args": [
              "--schema=/schema.sql",
              "--script-dir=/scripts/lua",
              "--output='$format'",
              "--from-server='$fromServer'",
              "--from-version='$fromVersion'",
              "--to-server='$toServer'",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timePrefix'",
              "--results-prefix='$actorPrefix'",
              "'"$averageTimeChangeQuery"'"
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
