nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
nativepath() { echo "$1"; }
nativevar() { eval export "$1"="$2"; }
skiponwindows() { :; }

IS_WINDOWS=false

if [ -d /mnt/*/Windows/System32 ]; then
    IS_WINDOWS=true
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
