load helper/windows-compat

# When DOLT_LEGACY_BIN is set (backward-compat run), old_dolt runs that binary; otherwise runs current dolt.
old_dolt() {
  if [ -n "$DOLT_LEGACY_BIN" ]; then
    "$DOLT_LEGACY_BIN" "$@"
  else
    dolt "$@"
  fi
}

if [ -z "$BATS_TMPDIR" ]; then
    export BATS_TMPDIR=$HOME/batstmp/
    mkdir $BATS_TMPDIR
fi

setup_common() {
    echo "setup" > /dev/null
}

teardown_common() {
    echo "teardown" > /dev/null
}

dolt config --global --add metrics.disabled true > /dev/null 2>&1
