#!/bin/sh

set -e

if [ "$#" -lt 6 ]; then
    echo  "Usage: ./get-job-json.sh <jobname> <version> <futureVersion> <timePrefix> <actorPrefix> <format> <initBigRepo> <nomsBinFormat>"
    exit 1
fi

jobName="$1"
version="$2"
futureVersion="$3"
timePrefix="$4"
actorPrefix="$5"
format="$6"
initBigRepo="$7"
nomsBinFormat="$8"

if [ -n "$initBigRepo" ]; then
  initBigRepo="\"--init-big-repo=$initBigRepo\","
fi

if [ -n "$nomsBinFormat" ]; then
  nomsBinFormat="\"--noms-bin-format=$nomsBinFormat\","
fi

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
      "metadata": {
        "annotations": {
          "alert_recipients": "'$ACTOR_EMAIL'"
        },
        "labels": {
          "k8s-liquidata-inc-monitored-job": "created-by-static-config",
          "app": "performance-benchmarking"
        }
      },
      "spec": {
        "serviceAccountName": "performance-benchmarking",
        "containers": [
          {
            "name": "performance-benchmarking",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/performance-benchmarking:latest",
            "resources": {
              "limits": {
                "cpu": "7000m"
              }
            },
            "env": [
              { "name": "GOMAXPROCS", "value": "7" },
              { "name": "ACTOR", "value": "'$ACTOR'" },
              { "name": "ACTOR_EMAIL", "value": "'$ACTOR_EMAIL'" },
              { "name": "REPO_ACCESS_TOKEN", "value": "'$REPO_ACCESS_TOKEN'" }
            ],
            "imagePullPolicy": "Always",
            "args": [
              "--schema=/app/schema.sql",
              "--useDoltHubLuaScriptsRepo",
              "--profile-version='"$version"'",
              "--future-version='"$futureVersion"'",
              "--bucket=performance-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timePrefix'",
              "--results-prefix='$actorPrefix'",
              '"$initBigRepo"'
              '"$nomsBinFormat"'
              "--output='$format'"
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
