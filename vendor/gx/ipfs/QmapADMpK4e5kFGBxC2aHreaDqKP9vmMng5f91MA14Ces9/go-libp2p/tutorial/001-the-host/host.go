package main

import (
	"context"
	"crypto/rand"
	"fmt"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	crypto "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	swarm "gx/ipfs/QmaijwHnbD4SabGA8C2fN9gchptLvRe2RxqTU5XkjAGBw5/go-libp2p-swarm"
	bhost "gx/ipfs/QmapADMpK4e5kFGBxC2aHreaDqKP9vmMng5f91MA14Ces9/go-libp2p/p2p/host/basic"
)

func main() {
	// Generate an identity keypair using go's cryptographic randomness source
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err)
	}

	// A peers ID is the hash of its public key
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		panic(err)
	}

	// We've created the identity, now we need to store it.
	// A peerstore holds information about peers, including your own
	ps := pstore.NewPeerstore()
	ps.AddPrivKey(pid, priv)
	ps.AddPubKey(pid, pub)

	maddr, err := ma.NewMultiaddr("/ip4/0.0.0.0/tcp/9000")
	if err != nil {
		panic(err)
	}

	// Make a context to govern the lifespan of the swarm
	ctx := context.Background()

	// Put all this together
	netw, err := swarm.NewNetwork(ctx, []ma.Multiaddr{maddr}, pid, ps, nil)
	if err != nil {
		panic(err)
	}

	myhost := bhost.New(netw)
	fmt.Printf("Hello World, my hosts ID is %s\n", myhost.ID())
}
