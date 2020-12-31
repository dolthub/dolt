#!/bin/sh

set -e

if [ "$#" -ne 5 ]; then
    echo  "Usage: ./get-pod-spec-json <podname> <fromVersion> <toVersion> <timeprefix> <actorprefix>"
    exit 1
fi

podname="$1"
fromVersion="$2"
toVersion="$3"
timeprefix="$4"
actorprefix="$5"

echo '
{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "'$podname'",
    "namespace": "performance-benchmarking"
  },
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
          "--results-dir='$timeprefix'",
          "--results-prefix='$actorprefix'",
          "select * from from_results;",
          "select * from to_results;"
        ]
      }
    ],
    "restartPolicy": "OnFailure"
  }
}
'
