#!/bin/sh

set -e

if [ "$#" -ne 6 ]; then
    echo  "Usage: ./get-dolt-correctness-job-json.sh <jobname> <fromVersion> <toVersion> <timeprefix> <actorprefix> <format>"
    exit 1
fi

jobname="$1"
fromVersion="$2"
toVersion="$3"
timeprefix="$4"
actorprefix="$5"
format="$6"

precision="2"

resultCountQuery="select result, count(*) as total from results where result != 'skipped' group by result;"
testCountQuery="select count(*) as total_tests from results where result != 'skipped';"
correctnessQuery="select ROUND(100.0 * (cast(ok_results.total as decimal) / (cast(all_results.total as decimal) + .000001)), $precision) as correctness_percentage from (select count(*) as total from results where result = 'ok') as ok_results join (select count(*) as total from results where result != 'skipped') as all_results"

echo '
{
  "apiVersion": "batch/v1",
  "kind": "Job",
  "metadata": {
    "name": "'$jobname'",
    "namespace": "sql-correctness"
  },
  "spec": {
    "backoffLimit": 3,
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
              "--schema=/correctness.sql",
              "--output='$format'",
              "--version='$toVersion'",
              "--bucket=sql-correctness-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "'"$resultCountQuery"'",
              "'"$testCountQuery"'",
              "'"$correctnessQuery"'"
            ]
          }
        ],
        "restartPolicy": "Never"
      }
    }
  }
}
'
