#!/bin/sh

set -e

if [ "$#" -ne 5 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <fromVersion> <toVersion> <timeprefix> <actorprefix>"
    exit 1
fi

jobname="$1"
fromVersion="0.22.5"
toVersion="0.22.6"
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
    "backoffLimit": 2,
    "template": {
      "spec": {
        "serviceAccountName": "performance-benchmarking",
        "containers": [
          {
            "name": "performance-benchmarking",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/performance-benchmarking:latest",
            "args": [
              "--from-version='$fromVersion'",
              "--to-version='$toVersion'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "select * from from_results;",
              "select * from to_results;"
            ]
          }
        ],
        "restartPolicy": "Never"
      }
    }
  }
}
'
