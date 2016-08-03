package unionfs

import "flag"

// VerboseTest returns true if the testing framework is run with -v.
func VerboseTest() bool {
	flag := flag.Lookup("test.v")
	return flag != nil && flag.Value.String() == "true"
}
