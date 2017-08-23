package testutil

import (
	"io"
	"math/rand"
	"time"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

func RandPeerID() (peer.ID, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	h, _ := mh.Sum(buf, mh.SHA2_256, -1)
	return peer.ID(h), nil
}

func RandTestKeyPair(bits int) (ci.PrivKey, ci.PubKey, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return ci.GenerateKeyPairWithReader(ci.RSA, bits, r)
}

func SeededTestKeyPair(seed int64) (ci.PrivKey, ci.PubKey, error) {
	r := rand.New(rand.NewSource(seed))
	return ci.GenerateKeyPairWithReader(ci.RSA, 512, r)
}
