#!/bin/sh

set -e

if [ ! -n "$1" ] || [ ! -n "$2" ] || [ ! -n "$3" ]; then
    echo  "Usage: ./get-pod-spec-json <podname> <fromVersion> <toVersion>"
    exit 1
fi

podname="$1"
fromVersion="$2"
toVersion="$3"

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
          "select * from from_results;",
          "select * from to_results;"
        ]
      }
    ],
    "restartPolicy": "OnFailure"
  }
}
'
