load $BATS_TEST_DIRNAME/helper/windows-compat.bash

nativebatsdir() { echo `nativepath $BATS_TEST_DIRNAME/$1`; }
batshelper() { echo `nativebatsdir helper/$1`; }
