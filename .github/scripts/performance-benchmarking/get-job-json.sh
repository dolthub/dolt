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
              "--from-version='$fromVersion'",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "select f.test_name as test_name, AVG(f.latency_sum_ms) as from_latency_sum_ms, AVG(f.sql_transactions_total) as from_sql_transactions_total, (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001)) as from_average_time, AVG(t.latency_sum_ms) as to_latency_sum_ms, AVG(t.sql_transactions_total) as to_sql_transactions_total, (AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) as to_average_time, (100 * (1.0 - ((AVG(t.latency_sum_ms) / (AVG(cast(t.sql_transactions_total as decimal)) + .000001)) / (AVG(f.latency_sum_ms) / (AVG(cast(f.sql_transactions_total as decimal)) + .000001))))) as from_to_average_time_percent_change from from_results as f join to_results as t on f.test_name = t.test_name group by f.test_name;"
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
