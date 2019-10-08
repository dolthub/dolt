load helper/windows-compat

if [ -z "$BATS_TMPDIR" ]; then
	export BATS_TMPDIR=$HOME/batstmp/
	mkdir $BATS_TMPDIR
fi

nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
batshelper() { echo `nativebatsdir helper/$1`; }

set_dolt_user() {
	dolt config --global --add user.name "$1" > /dev/null 2>&1
	dolt config --global --add user.email "$2" > /dev/null 2>&1
}

setup_common() {
	export PATH=$PATH:~/go/bin
	export NOMS_VERSION_NEXT=1
	cd $BATS_TMPDIR
	# Append the directory name with the pid of the calling process so
	# multiple tests can be run in parallel on the same machine
	mkdir "dolt-repo-$$"
	cd "dolt-repo-$$"

	SKIP_INIT=$1
	if [ "$SKIP_INIT" != true ] ; then
		dolt init
	fi
}

teardown_common() {
	rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-$$ /p
dolt config --global --add metrics.disabled true > /dev/null 2>&1
set_dolt_user "Bats Tests" "bats@email.fake" 
