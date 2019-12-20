#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

cp -f go/go.mod go/go.mod.bak
trap 'mv -f go/go.mod.bak go/go.mod' EXIT
grep -v 'replace github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi' go/go.mod.bak > go/go.mod
bazel run //:gazelle -- \
      update-repos \
      -from_file=go/go.mod \
      -build_file_proto_mode disable_global \
      -to_macro=bazel/go_repositories.bzl%go_repositories

h=`md5sum bazel/go_repositories.bzl`
sed -i '' 's|# .* bazel/go_repositories.bzl$|# '"$h"'|' WORKSPACE
