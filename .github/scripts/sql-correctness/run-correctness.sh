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

if [ -z "$NOMS_BIN_FORMAT" ]; then
    echo  "Must set NOMS_BIN_FORMAT"
    exit 1
fi

if [ -z "$VERSION" ]; then
    echo  "Must set VERSION for correctness run"
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

if [ -n "$PR_NUMBER" ]; then
  if [ -z "$REGRESS_COMP" ]; then
    echo "Must set REGRESS_COMP for PR correctness comparisons"
    exit 1
  fi
  if [ -z "$PR_BRANCH_REF" ]; then
    echo "Must set PR_BRANCH_REF for PR correctness comparisons"
    exit 1
  fi
fi

# use first 8 characters of VERSION to differentiate
# jobs
short=${VERSION:0:8}
lowered=$(echo "$ACTOR" | tr '[:upper:]' '[:lower:]')
actorShort="$lowered-$short"

# random sleep
sleep 0.$[ ( $RANDOM % 10 )  + 1 ]s

timesuffix=`date +%s%N`

jobname=""
if [ -n "$PR_NUMBER" ]; then
  jobname="$lowered-$PR_NUMBER"
else
  jobname="$actorShort-$timesuffix"
fi

timeprefix=$(date +%Y/%m/%d)

actorprefix="$MODE/$lowered/$jobname/$NOMS_BIN_FORMAT"

format="markdown"
if [[ "$MODE" = "release" || "$MODE" = "nightly" ]]; then
  format="html"
fi

# set value to PR_NUMBER environment variable
# or default to -1
issuenumber=${PR_NUMBER:-"-1"}

source \
  "$TEMPLATE_SCRIPT" \
  "$jobname" \
  "$VERSION" \
  "$timeprefix" \
  "$actorprefix" \
  "$format" \
  "$NOMS_BIN_FORMAT" \
  "$issuenumber" \
  "$REGRESS_COMP" \
  "$PR_BRANCH_REF" > job.json

# delete existing job with same name if this is a pr job
if [ -n "$PR_NUMBER" ]; then
  out=$(KUBECONFIG="$KUBECONFIG" kubectl delete job/"$jobname" -n sql-correctness || true)
  echo "Delete pr job if exists: $out"
fi

out=$(KUBECONFIG="$KUBECONFIG" kubectl apply -f job.json || true)

if [ "$out" != "job.batch/$jobname created" ]; then
  echo "something went wrong creating job... this job likely already exists in the cluster"
  echo "$out"
  exit 1
else
  echo "$out"
fi

exit 0
