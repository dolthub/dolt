#!/bin/sh

set -e

if [ "$#" -ne 5 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <fromVersion> <toVersion> <timeprefix> <actorprefix>"
    exit 1
fi

jobname="$1"
fromVersion="$2"
toVersion="$3"
timeprefix="$4"
actorprefix="$5"

average_time_change_query="select f.test_name as test_name, ROUND(100 * (1.0 - ((AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) / (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001))))) as average_time_percent_change, case when (100 * (1.0 - ((AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) / (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001))))) < 0 then true else false end as is_faster from from_results as f join to_results as t on f.test_name = t.test_name group by f.test_name;"

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
            "args": [
              "--schema=/schema.sql",
              "--from-server=dolt",
              "--from-version='$fromVersion'",
              "--to-server=dolt",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "'"$average_time_change_query"'"
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
