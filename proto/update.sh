#!/usr/bin/env bash

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

GO_ROOT="$BUILD_WORKSPACE_DIRECTORY""/../go/"
PBGO_TAR_FILE=$(rlocation "$1" | sed 's|C:|/c|')
FBGO_TAR_FILE=$(rlocation "$2" | sed 's|C:|/c|')
GOIMPORTS_BIN=$(rlocation "$3" | sed 's|C:|/c|')

cd "$GO_ROOT"

# First, clean up any existing files.
find . -name '*.pb.go' -exec rm -f \{\} \;
# XXX: Pretty gross :-/.
rm -rf gen/fb

# Then unpack generated sources into the correct place.
tar -x -f "$PBGO_TAR_FILE"
tar -x -f "$FBGO_TAR_FILE"

"$GOIMPORTS_BIN" -w ./gen/
