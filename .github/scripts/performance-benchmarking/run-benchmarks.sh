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

if [ -n "$PROFILE" ]; then
    if [ -z "$FROM_VERSION" ]; then
        echo  "Must set FROM_VERSION"
        exit 1
    fi
    if [ -z "$FUTURE_VERSION" ]; then
        echo "Setting FUTURE_VERSION to $FROM_VERSION"
        export FUTURE_VERSION="$FROM_VERSION"
    fi

    echo "Setting profile version to $FROM_VERSION"
else
  if [ -z "$FROM_SERVER" ] || [ -z "$FROM_VERSION" ] || [ -z "$TO_SERVER" ] || [ -z "$TO_VERSION" ]; then
      echo  "Must set FROM_SERVER FROM_VERSION TO_SERVER and TO_VERSION"
      exit 1
  fi

  echo "Setting from $FROM_SERVER: $FROM_VERSION"
  echo "Setting to $TO_SERVER: $TO_VERSION"
fi

if [ -z "$ACTOR" ]; then
    echo  "Must set ACTOR"
    exit 1
fi

if [ -z "$MODE" ]; then
    echo  "Must set MODE"
    exit 1
fi

if [ "$NOMS_BIN_FORMAT" = "__DOLT__" ]; then
  INIT_BIG_REPO="false"
fi

# use first 8 characters of FROM_VERSION | TO_VERSION to differentiate
# jobs
short=${TO_VERSION:0:8}
if [ -n "$PROFILE" ]; then
  short=${FROM_VERSION:0:8}
fi

lowered=$(echo "$ACTOR" | tr '[:upper:]' '[:lower:]')
actorShort="$lowered-$short"

# random sleep
sleep 0.$[ ( $RANDOM % 10 )  + 1 ]s

timesuffix=`date +%s%N`

jobname="$actorShort"
if [ -n "$WITH_TPCC" ]; then
  jobname="$jobname-tpcc"
elif [ -n "$PROFILE" ]; then
  jobname="$jobname-profile"
fi

jobname="$jobname-$timesuffix"

timeprefix=$(date +%Y/%m/%d)

actorprefix="$MODE/$lowered/$jobname/$NOMS_BIN_FORMAT"

format="markdown"
if [[ "$MODE" = "release" || "$MODE" = "nightly" ]]; then
  format="html"
fi

export TO_PROFILE_KEY

# set value to ISSUE_NUMBER environment variable
# or default to -1
issuenumber=${ISSUE_NUMBER:-"-1"}

if [ -n "$PROFILE" ]; then
source \
    "$TEMPLATE_SCRIPT" \
    "$jobname"         \
    "$FROM_VERSION"    \
    "$FUTURE_VERSION"  \
    "$timeprefix"      \
    "$actorprefix"     \
    "$format"          \
    "$INIT_BIG_REPO"   \
    "$NOMS_BIN_FORMAT" > job.json
else
  source \
    "$TEMPLATE_SCRIPT"    \
    "$jobname"            \
    "$FROM_SERVER"        \
    "$FROM_VERSION"       \
    "$TO_SERVER"          \
    "$TO_VERSION"         \
    "$timeprefix"         \
    "$actorprefix"        \
    "$format"             \
    "$issuenumber"        \
    "$INIT_BIG_REPO"      \
    "$NOMS_BIN_FORMAT"    \
    "$SYSBENCH_TEST_TIME" \
    "$WITH_TPCC"          > job.json
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
