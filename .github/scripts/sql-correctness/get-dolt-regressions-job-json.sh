#!/bin/sh

set -e

if [ "$#" -ne 6 ]; then
    echo  "Usage: ./get-dolt-regressions-job-json.sh <jobname> <fromVersion> <toVersion> <timeprefix> <actorprefix> <format>"
    exit 1
fi

jobname="$1"
fromVersion="$2"
toVersion="$3"
timeprefix="$4"
actorprefix="$5"
format="$6"

regressionsQuery="select count(*) from from_results;"

echo '
{
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "'$jobname'",
    "namespace": "sql-correctness"
  },
  "spec": {
    "backoffLimit": 1,
    "template": {
      "spec": {
        "serviceAccountName": "sql-correctness",
        "containers": [
          {
            "name": "sql-correctness",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/sql-correctness:latest",
            "env": [
              { "name": "REPO_ACCESS_TOKEN", "value": "'$REPO_ACCESS_TOKEN'"},
              { "name": "ACTOR", "value": "'$ACTOR'"},
              { "name": "ACTOR_EMAIL", "value": "'$ACTOR_EMAIL'"}
            ],
            "args": [
              "--schema=/regressions.sql",
              "--output='$format'",
              "--from-version='$fromVersion'",
              "--to-version='$toVersion'",
              "--bucket=sql-correctness-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "'"$regressionsQuery"'"
            ]
          }
        ],
        "restartPolicy": "Never"
      }
    }
  }
}
'
