package commands

import (
	"testing"

	"github.com/ipfs/go-ipfs/namesys"
	tu "github.com/ipfs/go-ipfs/thirdparty/testutil"
)

func TestKeyTranslation(t *testing.T) {
	pid := tu.RandPeerIDFatal(t)
	a, b := namesys.IpnsKeysForID(pid)

	pkk, err := escapeDhtKey("/pk/" + pid.Pretty())
	if err != nil {
		t.Fatal(err)
	}

	ipnsk, err := escapeDhtKey("/ipns/" + pid.Pretty())
	if err != nil {
		t.Fatal(err)
	}

	if pkk != a {
		t.Fatal("keys didnt match!")
	}

	if ipnsk != b {
		t.Fatal("keys didnt match!")
	}
}
