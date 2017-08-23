package dht_pb

import (
	"testing"
)

func TestBadAddrsDontReturnNil(t *testing.T) {
	mp := new(Message_Peer)
	mp.Addrs = [][]byte{[]byte("NOT A VALID MULTIADDR")}

	addrs := mp.Addresses()
	if len(addrs) > 0 {
		t.Fatal("shouldnt have any multiaddrs")
	}
}
