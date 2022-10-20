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

if [ -z "$VERSION" ]; then
    echo  "Must set VERSION"
    exit 1
fi

echo "using dolt version: $VERSION"

if [ -z "$ACTOR" ]; then
    echo  "Must set ACTOR"
    exit 1
fi

nomsFormat="ldnbf"
if [ "$NOMS_BIN_FORMAT" == "__DOLT__"]; then
  nomsFormat="doltnbf"
fi

# use first 8 characters of TO_VERSION to differentiate
# jobs
short=${VERSION:0:8}
lowered=$(echo "$ACTOR" | tr '[:upper:]' '[:lower:]')
actorShort="$lowered-$nomsFormat-$short"

jobname="$actorShort"

source "$TEMPLATE_SCRIPT" "$jobname" "$VERSION" > job.json

out=$(KUBECONFIG="$KUBECONFIG" kubectl apply -f job.json || true)

if [ "$out" != "job.batch/$jobname created" ]; then
  echo "something went wrong creating job... this job likely already exists in the cluster"
  echo "$out"
  exit 1
else
  echo "$out"
fi

exit 0
