# Helpers for the cross-version compatibility suite. Pair with the standard
# suite's common.bash to inherit setup_common, teardown_common, and the
# DOLT_ROOT_PATH plus user config:
#
#   bats_load_library common.bash
#   bats_load_library compat-common.bash
#
# Required environment:
#   DOLT_OLD_BIN          older dolt binary; falls back to dolt on PATH.
#   DOLT_NEW_BIN          newer dolt binary; falls back to dolt on PATH.
#   DOLT_DEV_BUILD_PATH   optional. Set by the runner to the freshly built
#                         dolt so version skips do not skip the dev build.

old_dolt() {
  if [ -n "$DOLT_OLD_BIN" ]; then
    "$DOLT_OLD_BIN" "$@"
  else
    dolt "$@"
  fi
}

new_dolt() {
  if [ -n "$DOLT_NEW_BIN" ]; then
    "$DOLT_NEW_BIN" "$@"
  else
    dolt "$@"
  fi
}

is_dev_build() {
  [ -n "${DOLT_DEV_BUILD_PATH:-}" ] && [ "$1" = "$DOLT_DEV_BUILD_PATH" ]
}

# skip_if_new_lte(<max_version>, <reason>) skips when new_dolt is at or below
# |max_version|. The dev build is exempt only when its reported version is
# already at or above |max_version|, so a freshly built dolt is not skipped
# by literal version comparisons against an unreleased number.
skip_if_new_lte() {
  local max_version="$1"
  local reason="$2"
  local new_version
  new_version=$(new_dolt version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -z "$new_version" ]; then
    return 0
  fi
  if is_dev_build "${DOLT_NEW_BIN:-}" && [ "$(printf '%s\n' "$new_version" "$max_version" | sort -V | head -n1)" = "$max_version" ]; then
    return 0
  fi
  if [ "$(printf '%s\n' "$max_version" "$new_version" | sort -V | head -n1)" = "$new_version" ]; then
    skip "$reason (new_dolt version: $new_version)"
  fi
}

# skip_if_old_lte(<max_version>, <reason>) skips when old_dolt is at or below
# |max_version|. See skip_if_new_lte for the dev build exemption.
skip_if_old_lte() {
  local max_version="$1"
  local reason="$2"
  local old_version
  old_version=$(old_dolt version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -z "$old_version" ]; then
    return 0
  fi
  if is_dev_build "${DOLT_OLD_BIN:-}" && [ "$(printf '%s\n' "$old_version" "$max_version" | sort -V | head -n1)" = "$max_version" ]; then
    return 0
  fi
  if [ "$(printf '%s\n' "$max_version" "$old_version" | sort -V | head -n1)" = "$old_version" ]; then
    skip "$reason (old_dolt version: $old_version)"
  fi
}

strip_ansi() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g'
}

extract_commit_hash() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g' | grep -m1 '^commit ' | awk '{print $2}'
}
