#!/bin/bash

retry() {
  for i in {1..3}; do
    "${@}" && return 0
  done
  return 1
}

# Fail on any error.
set -eo pipefail

# Display commands being run.
set -x

cd git/gocloud
COMMIT=$(git rev-parse HEAD)

# Set $GOPATH
export GOPATH="$HOME/go"
GOCLOUD_HOME=$GOPATH/src/cloud.google.com/go
mkdir -p $GOCLOUD_HOME

# Move code into $GOPATH and get dependencies
cp -R ./* $GOCLOUD_HOME
cd $GOCLOUD_HOME/internal/kokoro
# Don't print out encryption keys, etc
set +x
key=$(cat "$KOKORO_ARTIFACTS_DIR/keystore/72523_encrypted_ba2d6f7723ed_key")
iv=$(cat "$KOKORO_ARTIFACTS_DIR/keystore/72523_encrypted_ba2d6f7723ed_iv")
pass=$(cat "$KOKORO_ARTIFACTS_DIR/keystore/72523_encrypted_ba2d6f7723ed_pass")

openssl aes-256-cbc -K $key -iv $iv -pass pass:$pass -in kokoro-key.json.enc -out key.json -d
set -x

export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/key.json"
export GCLOUD_TESTS_GOLANG_PROJECT_ID="dulcet-port-762"
export GCLOUD_TESTS_GOLANG_ZONE="us-west1-b"
export GCLOUD_TESTS_GOLANG_BUCKET="dulcet-port-762-go-cloud-profiler-test"

cd $GOCLOUD_HOME/profiler
retry go get -t -tags=integration .
go test -timeout=60m -tags=integration -run TestAgentIntegration -commit="$COMMIT"
