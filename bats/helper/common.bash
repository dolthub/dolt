load helper/windows-compat

if [ -z "$BATS_TMPDIR" ]; then
	export BATS_TMPDIR=$HOME/batstmp/
	mkdir $BATS_TMPDIR
fi

nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-$$ /p
dolt config --global --add user.name "Bats Tests"
dolt config --global --add user.email "bats@email.fake"
dolt config --global --add metrics.disabled true

nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
batshelper() { echo `nativebatsdir helper/$1`; }
