skiponwindows() { :; }
nativepath() { echo "$1"; }

if [ -d /mnt/*/Windows/System32 ]; then
	nativepath() {
		wslpath -w "$1"
	}
    skiponwindows() {
        skip "$1"
    }
fi
