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

medianLatencyMultiplierQuery="select f.test_name as test_name, avg(f.latency_percentile) as from_latency_median, avg(t.latency_percentile) as to_latency_median, ROUND(avg(f.latency_percentile) / (avg(t.latency_percentile) + .000001)) as multiplier from from_results as f join to_results as t on f.test_name = t.test_name group by f.test_name;"

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
              "'"$medianLatencyMultiplierQuery"'"
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
