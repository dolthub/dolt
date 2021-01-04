#!/bin/bash

set -e

if [ -z "$KUBECONFIG" ]; then
    echo  "Must set KUBECONFIG"
    exit 1
fi

if [ -z "$TEMPLATE_SCRIPT" ]; then
    echo  "Must set TEMPLATE_SCRIPT"
    exit 1
fi

if [ -z "$FROM_VERSION" ] || [ -z "$TO_VERSION" ]; then
    echo  "Must set FROM_VERSION and TO_VERSION"
    exit 1
fi

if [ -z "$ACTOR" ]; then
    echo  "Must set ACTOR"
    exit 1
fi

echo "Setting from version to: $FROM_VERSION"
echo "Setting to version to: $TO_VERSION"

jobname="performance-benchmarking-$ACTOR"

timeprefix=$(date +%Y/%m/%d)

actorprefix="$ACTOR/$TO_VERSION"

source "$TEMPLATE_SCRIPT" "$jobname" "$FROM_VERSION" "$TO_VERSION" "$timeprefix" "$actorprefix" > job.json

KUBECONFIG="$KUBECONFIG" kubectl apply -f job.json

errors=$(KUBECONFIG="$KUBECONFIG" kubectl wait job/"$jobname" --for=condition=complete -n performance-benchmarking --timeout=400s 2>&1 || true)

if [ -z "$errors" ]; then
  KUBECONFIG="$KUBECONFIG" kubectl delete job/"$jobname" -n performance-benchmarking
  echo "::set-output name=object-key::$timeprefix/$actorprefix/compare-results.log"
  exit 0
else
  KUBECONFIG="$KUBECONFIG" kubectl logs job/"$jobname" -n performance-benchmarking
  exit 1
fi
