package flags

import (
	"os"
	"testing"
)

// This variable is initialized before flags init(), so we export the ENV variable here.
var _lowMemOn = lowMemOn()

func lowMemOn() error {
	os.Setenv("IPFS_LOW_MEM", "true")
	return nil
}

func TestLowMemMode(t *testing.T) {
	if !LowMemMode {
		t.Fatal("LowMemMode does not turn on even with 'IPFS_LOW_MEM' ENV variable set.")
	}
}
