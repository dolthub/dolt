package record

import (
	"encoding/base64"
	"testing"

	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

var OffensiveKey = "CAASXjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQDjXAQQMal4SB2tSnX6NJIPmC69/BT8A8jc7/gDUZNkEhdhYHvc7k7S4vntV/c92nJGxNdop9fKJyevuNMuXhhHAgMBAAE="

func TestValidatePublicKey(t *testing.T) {
	pkb, err := base64.StdEncoding.DecodeString(OffensiveKey)
	if err != nil {
		t.Fatal(err)
	}

	pubk, err := ci.UnmarshalPublicKey(pkb)
	if err != nil {
		t.Fatal(err)
	}

	pkb2, err := pubk.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	pkh := u.Hash(pkb2)

	k := "/pk/" + string(pkh)

	err = ValidatePublicKeyRecord(k, pkb)
	if err != nil {
		t.Fatal(err)
	}
}
