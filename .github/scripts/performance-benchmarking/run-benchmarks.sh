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

# set value to ISSUE_NUMBER environment variable
# or default to -1
issuenumber=${ISSUE_NUMBER:-"-1"}

source "$TEMPLATE_SCRIPT" "$jobname" "$FROM_SERVER" "$FROM_VERSION" "$TO_SERVER" "$TO_VERSION" "$timeprefix" "$actorprefix" "$format" "$issuenumber" > job.json

out=$(KUBECONFIG="$KUBECONFIG" kubectl apply -f job.json || true)

if [ "$out" != "job.batch/$jobname created" ]; then
  echo "something went wrong creating job... this job likely already exists in the cluster"
  echo "$out"
  exit 1
else
  echo "$out"
fi

exit 0
