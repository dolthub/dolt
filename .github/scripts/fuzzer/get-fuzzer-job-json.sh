#!/bin/sh

set -e

if [ "$#" -ne 2 ]; then
    echo  "Usage: ./get-dolt-correctness-job-json.sh <jobname> <version>"
    exit 1
fi

jobname="$1"
version="$2"

echo '
{
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "'$jobname'",
    "namespace": "fuzzer"
  },
  "spec": {
    "backoffLimit": 1,
    "template": {
      "spec": {
        "serviceAccountName": "fuzzer",
        "containers": [
          {
            "name": "fuzzer",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/fuzzer:latest",
            "env": [
              { "name": "REPO_ACCESS_TOKEN", "value": "'$REPO_ACCESS_TOKEN'"},
              { "name": "ACTOR", "value": "'$ACTOR'"},
              { "name": "DOLT_BIN", "value": "/usr/local/bin"}
            ],
            "args": [
              "--dolt-version='$version'",
              "--bucket=dolt-fuzzer-runs",
              "--region=us-west-2",
              "--fuzzer-args=basic, --cycles=4, --config=/configs/scheduled-job.toml"
            ]
          }
        ],
        "restartPolicy": "Never"
      }
    }
  }
}
'
