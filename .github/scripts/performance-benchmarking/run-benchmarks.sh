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

if [ -z "$FROM_SERVER" ] || [ -z "$FROM_VERSION" ] || [ -z "$TO_SERVER" ] || [ -z "$TO_VERSION" ]; then
    echo  "Must set FROM_SERVER FROM_VERSION TO_SERVER and TO_VERSION"
    exit 1
fi

if [ -z "$ACTOR" ]; then
    echo  "Must set ACTOR"
    exit 1
fi

if [ -z "$MODE" ]; then
    echo  "Must set MODE"
    exit 1
fi

echo "Setting from $FROM_SERVER: $FROM_VERSION"
echo "Setting to $TO_SERVER: $TO_VERSION"

jobname="performance-benchmarking-$ACTOR"

timeprefix=$(date +%Y/%m/%d)

actorprefix="$MODE/$ACTOR/$TO_VERSION"

format="markdown"
if [ "$MODE" = "release" ]; then
  format="html"
fi

source "$TEMPLATE_SCRIPT" "$jobname" "$FROM_SERVER" "$FROM_VERSION" "$TO_SERVER" "$TO_VERSION" "$timeprefix" "$actorprefix" "$format" > job.json

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
