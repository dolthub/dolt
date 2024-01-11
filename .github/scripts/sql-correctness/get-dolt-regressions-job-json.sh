#!/bin/sh

set -e

if [ "$#" -lt 6 ]; then
    echo  "Usage: ./get-dolt-correctness-job-json.sh <jobname> <fromVersion> <toVersion> <timeprefix> <actorprefix> <format> <nomsBinFormat> <issueNumber>"
    exit 1
fi

jobname="$1"
fromVersion="$2"
toVersion="$3"
timeprefix="$4"
actorprefix="$5"
format="$6"
nomsBinFormat="$7"
issueNumber="$8"

precision="6"

if [ -n "$nomsBinFormat" ]; then
  nomsBinFormat="\"--noms-bin-format=$nomsBinFormat\","
fi

regressionsCountQuery="select count(*) as correctness_regressions from from_results as f join to_results t on f.test_file = t.test_file and f.line_num = t.line_num and f.result = 'ok' and t.result != 'ok';"
improvementsCountQuery="select count(*) as correctness_improvements from from_results as f join to_results t on f.test_file = t.test_file and f.line_num = t.line_num and f.result != 'ok' and t.result = 'ok';"
fromResultsCountQuery="select result as from_version_result, count(*) as from_version_total from from_results where result != 'skipped' group by result;"
toResultsCountQuery="select result as to_version_result, count(*) as to_version_total from to_results where result != 'skipped' group by result;"
testCountQuery="select count(*) as total_tests from from_results where result != 'skipped';"
fromCorrectnessQuery="select ROUND(100.0 * (cast(ok_results.total as decimal) / (cast(all_results.total as decimal) + .000001)), $precision) as from_correctness_percentage from (select count(*) as total from from_results where result = 'ok') as ok_results join (select count(*) as total from from_results where result != 'skipped') as all_results"
toCorrectnessQuery="select ROUND(100.0 * (cast(ok_results.total as decimal) / (cast(all_results.total as decimal) + .000001)), $precision) as to_correctness_percentage from (select count(*) as total from to_results where result = 'ok') as ok_results join (select count(*) as total from to_results where result != 'skipped') as all_results"

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
          "k8s-liquidata-inc-monitored-job": "created-by-static-config"
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
              "--schema=/regressions.sql",
              "--concurrent",
              "--concurrent-limit=25"
              "--output='$format'",
              "--to-version='$toVersion'",
              "--from-version='$fromVersion'",
              '"$nomsBinFormat"'
              "--bucket=sql-correctness-github-actions-results",
              "--region=us-west-2",
              "--issue-number='$issueNumber'",
              "--results-dir='$timeprefix'",
              "--results-prefix='$actorprefix'",
              "'"$regressionsCountQuery"'",
              "'"$improvementsCountQuery"'",
              "'"$fromResultsCountQuery"'",
              "'"$toResultsCountQuery"'",
              "'"$testCountQuery"'",
              "'"$fromCorrectnessQuery"'",
              "'"$toCorrectnessQuery"'"
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
