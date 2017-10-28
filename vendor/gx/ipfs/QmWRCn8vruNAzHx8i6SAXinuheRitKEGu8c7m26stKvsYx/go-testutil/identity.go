package testutil

import (
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

type Identity interface {
	Address() ma.Multiaddr
	ID() peer.ID
	PrivateKey() ci.PrivKey
	PublicKey() ci.PubKey
}

// TODO add a cheaper way to generate identities

func RandIdentity() (Identity, error) {
	p, err := RandPeerNetParams()
	if err != nil {
		return nil, err
	}
	return &identity{*p}, nil
}

func RandIdentityOrFatal(t *testing.T) Identity {
	p, err := RandPeerNetParams()
	if err != nil {
		t.Fatal(err)
	}
	return &identity{*p}
}

// identity is a temporary shim to delay binding of PeerNetParams.
type identity struct {
	PeerNetParams
}

func (p *identity) ID() peer.ID {
	return p.PeerNetParams.ID
}

func (p *identity) Address() ma.Multiaddr {
	return p.Addr
}

func (p *identity) PrivateKey() ci.PrivKey {
	return p.PrivKey
}

func (p *identity) PublicKey() ci.PubKey {
	return p.PubKey
}
