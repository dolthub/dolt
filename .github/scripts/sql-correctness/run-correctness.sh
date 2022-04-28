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

if [ -z "$FROM_VERSION" ] && [ -z "$TO_VERSION" ]; then
    echo  "Must set FROM_VERSION or TO_VERSION for correctness run"
    echo  "Must set both for regressions run"
    exit 1
fi

if  [ ! -z "$FROM_VERSION" ] && [ -z "$TO_VERSION" ]; then
  echo "Setting TO_VERSION for correctness run"
  TO_VERSION="$FROM_VERSION"
fi

if [ -z "$ACTOR" ]; then
    echo  "Must set ACTOR"
    exit 1
fi

if [ -z "$MODE" ]; then
    echo  "Must set MODE"
    exit 1
fi

# use first 8 characters of TO_VERSION to differentiate
# jobs
short=${TO_VERSION:0:8}
lowered=$(echo "$ACTOR" | tr '[:upper:]' '[:lower:]')
actorShort="$lowered-$short"

jobname="$actorShort"

timeprefix=$(date +%Y/%m/%d)

actorprefix="$MODE/$ACTOR/$actorShort"

format="markdown"
if [[ "$MODE" = "release" || "$MODE" = "nightly" ]]; then
  format="html"
fi

source "$TEMPLATE_SCRIPT" "$jobname" "$FROM_VERSION" "$TO_VERSION" "$timeprefix" "$actorprefix" "$format" > job.json

out=$(KUBECONFIG="$KUBECONFIG" kubectl apply -f job.json || true)

if [ "$out" != "job.batch/$jobname created" ]; then
  echo "something went wrong creating job... this job likely already exists in the cluster"
  echo "$out"
  exit 1
else
  echo "$out"
fi

exit 0
