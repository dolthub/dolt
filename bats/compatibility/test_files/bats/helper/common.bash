load helper/windows-compat

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
