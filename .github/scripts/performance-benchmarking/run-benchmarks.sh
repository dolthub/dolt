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

out=$(KUBECONFIG="$KUBECONFIG" kubectl wait job/"$jobname" --for=condition=complete -n performance-benchmarking --timeout=1500s || true)

if [ "$out" != "job.batch/$jobname condition met" ]; then
  echo "output of kubectl wait: $out"
  KUBECONFIG="$KUBECONFIG" kubectl logs job/"$jobname" -n performance-benchmarking
else
  echo "::set-output name=object-key::$timeprefix/$actorprefix/comparison-results.log"
fi

KUBECONFIG="$KUBECONFIG" kubectl delete job/"$jobname" -n performance-benchmarking

exit 0
