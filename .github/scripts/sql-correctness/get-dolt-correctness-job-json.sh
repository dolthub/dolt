#!/bin/sh

set -e

if [ "$#" -lt 5 ]; then
    echo  "Usage: ./get-dolt-correctness-job-json.sh <jobname> <version> <timeprefix> <actorprefix> <format> <nomsBinFormat> <issueNumber> <regressComp> <branchRef>"
    exit 1
fi

jobname="$1"
version="$2"
timeprefix="$3"
actorprefix="$4"
format="$5"
nomsBinFormat="$6"
issueNumber="$7"
regressComp="$8"
branchRef="$9"

precision="6"

if [ -n "$nomsBinFormat" ]; then
  nomsBinFormat="\"--noms-bin-format=$nomsBinFormat\","
fi

if [ -n "$issueNumber" ]; then
  issueNumber="\"--issue-number=$issueNumber\","
fi

regressPrec=""
if [ -n "$regressComp" ]; then
  regressComp="\"--regress-compare=$regressComp\","
  regressPrec="\"--regress-precision=$precision\","
  branchRef="\"--branch=$branchRef\","
fi

resultCountQuery="select version, result, count(*) as total from results where result != 'skipped' group by result;"
testCountQuery="select version, count(*) as total_tests from results where result != 'skipped';"
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
      "metadata": {
        "annotations": {
          "alert_recipients": "'$ACTOR_EMAIL'"
        },
        "labels": {
          "k8s-liquidata-inc-monitored-job": "created-by-static-config",
          "app": "sql-correctness"
        }
      },
      "spec": {
        "serviceAccountName": "sql-correctness",
        "containers": [
          {
            "name": "sql-correctness",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/sql-correctness:latest",
            "resources": {
              "limits": {
                "cpu": "7000m"
              }
            },
            "env": [
              { "name": "REPO_ACCESS_TOKEN", "value": "'$REPO_ACCESS_TOKEN'"},
              { "name": "ACTOR", "value": "'$ACTOR'"},
              { "name": "ACTOR_EMAIL", "value": "'$ACTOR_EMAIL'"},
              { "name": "DOLT_DEFAULT_BIN_FORMAT", "value": "'$NOMS_BIN_FORMAT'"}
            ],
            "args": [
              "--schema=/app/correctness.sql",
              "--concurrent",
              "--output='$format'",
              "--version='$version'",
              "--timeout=600",
              '"$nomsBinFormat"'
              '"$issueNumber"'
              '"$regressComp"'
              '"$regressPrec"'
              '"$branchRef"'
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
        "restartPolicy": "Never",
        "nodeSelector": {
          "sql-correctness-worker": "true"
        },
        "tolerations": [
          {
              "effect": "NoSchedule",
              "key": "dedicated",
              "operator": "Equal",
              "value": "sql-correctness-worker"
          }
        ]
      }
    }
  }
}
'
