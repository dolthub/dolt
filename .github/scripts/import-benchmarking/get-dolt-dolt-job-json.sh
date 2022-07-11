#!/bin/sh

set -e

if [ "$#" -lt 6 ]; then
    echo  "Usage: ./get-dolt-dolt-job-json.sh <jobName> <fromVersion> <toVersion> <timePrefix> <actorPrefix> <issueNumber>"
    exit 1
fi

jobName="$1"
fromVersion="$2"
toVersion="$3"
timePrefix="$4"
actorPrefix="$5"
issueNumber="$6" # TODO: Use this to paste the results onto the github issue

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
      "spec": {
        "serviceAccountName": "import-benchmarking",
        "containers": [
          {
            "name": "import-benchmarking",
            "image": "407903926827.dkr.ecr.us-west-2.amazonaws.com/liquidata/import-benchmarking:latest",
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
              "--from-version='$fromVersion'",
              "--to-version='$toVersion'",
              "--bucket=import-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timePrefix'",
              "--results-prefix='$actorPrefix'",
              "--fileNames=100k-sorted.csv",
              "--fileNames=100k-random.csv",
              "--fileNames=1m-sorted.csv",
              "--fileNames=1m-random.csv",
              "--fileNames=10m-sorted.csv",
              "--fileNames=10m-random.csv"
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
