load helper/windows-compat

if [ -z "$BATS_TMPDIR" ]; then
	export BATS_TMPDIR=$HOME/batstmp/
	mkdir $BATS_TMPDIR
fi

nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
batshelper() { echo `nativebatsdir helper/$1`; }
