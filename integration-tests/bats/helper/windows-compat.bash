nativepath() { echo "$1"; }
nativevar() { eval export "$1"="$2"; }
skiponwindows() { :; }

IS_WINDOWS=${IS_WINDOWS:-false}
WINDOWS_BASE_DIR=${WINDOWS_BASE_DIR:-/mnt/c}

if [ -d "$WINDOWS_BASE_DIR"/Windows/System32 ]  || [ "$IS_WINDOWS" == true ]; then
    IS_WINDOWS=true
    if [ ! -d "$WINDOWS_BASE_DIR"/batstmp ]; then
        mkdir "$WINDOWS_BASE_DIR"/batstmp
    fi
    BATS_TMPDIR=`TMPDIR="$WINDOWS_BASE_DIR"/batstmp mktemp -d -t dolt-bats-tests-XXXXXX`
    export BATS_TMPDIR
    nativepath() {
        wslpath -w "$1"
    }
    nativevar() {
        eval export "$1"="$2"
        export WSLENV="$1$3"
    }
    skiponwindows() {
        skip "$1"
    }
fi
