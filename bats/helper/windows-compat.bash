nativepath() { echo "$1"; }
nativevar() { eval export "$1"="$2"; }
skiponwindows() { :; }

IS_WINDOWS=false

if [ -d /mnt/c/Windows/System32 ]; then
    IS_WINDOWS=true
    if [ ! -d /mnt/c/batstmp ]; then
        mkdir /mnt/c/batstmp
    fi
    BATS_TMPDIR=`TMPDIR=/mnt/c/batstmp mktemp -d -t dolt-bats-tests-XXXXXX`
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
