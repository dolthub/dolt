#!/bin/sh

set -e

if [ "$#" -lt 8 ]; then
    echo  "Usage: ./get-mysql-dolt-job-json.sh <jobName> <fromServer> <fromVersion> <toServer> <toVersion> <timePrefix> <actorPrefix> <issueNumber>"
    exit 1
fi

jobName="$1"
fromServer="$2"
fromVersion="$3"
toServer="$4"
toVersion="$5"
timePrefix="$6"
actorPrefix="$7"
issueNumber="$8" # TODO: Use this to paste the results onto the github issue

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
              "--from-server='$fromServer'",
              "--from-version='$fromVersion'",
              "--to-server='$toServer'",
              "--to-version='$toVersion'",
              "--bucket=import-benchmarking-github-actions-results",
              "--region=us-west-2",
              "--results-dir='$timePrefix'",
              "--results-prefix='$actorPrefix'",
              "--mysql-exec=/usr/sbin/mysqld",
              "--mysql-schema-file=schema.sql",
              "--fileNames=100k-sorted.csv",
              "--fileNames=100k-random.csv",
              "--fileNames=1m-sorted.csv",
              "--fileNames=1m-random.csv"
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
