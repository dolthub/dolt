package fsrepo

import (
	"os"
	"testing"

	config "github.com/ipfs/go-ipfs/repo/config"
)

func TestConfig(t *testing.T) {
	const filename = ".ipfsconfig"
	cfgWritten := new(config.Config)
	cfgWritten.Identity.PeerID = "faketest"

	err := WriteConfigFile(filename, cfgWritten)
	if err != nil {
		t.Fatal(err)
	}
	cfgRead, err := Load(filename)
	if err != nil {
		t.Fatal(err)
	}
	if cfgWritten.Identity.PeerID != cfgRead.Identity.PeerID {
		t.Fatal()
	}
	st, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("cannot stat config file: %v", err)
	}
	if g := st.Mode().Perm(); g&0117 != 0 {
		t.Fatalf("config file should not be executable or accessible to world: %v", g)
	}
}
