#!/bin/bash

# Copyright 2017 The Bazel Authors. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# autogazelle.bash is a bazel wrapper script that runs gazelle automatically
# before running bazel commands. See autogazelle.go for details.
#
# This script may be installed at tools/bazel in your workspace. It must
# be executable.

set -euo pipefail

case "${1:-}" in
  build|coverage|cquery|fetch|mobile-install|print_action|query|run|test)
    "$BAZEL_REAL" run @bazel_gazelle//cmd/autogazelle -- -gazelle=//:gazelle
    echo "done running autogazelle" 1>&2
    ;;
esac

exec "$BAZEL_REAL" "$@"
