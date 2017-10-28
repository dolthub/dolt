package mocknet

import (
	"context"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("mocknet")

// WithNPeers constructs a Mocknet with N peers.
func WithNPeers(ctx context.Context, n int) (Mocknet, error) {
	m := New(ctx)
	for i := 0; i < n; i++ {
		if _, err := m.GenPeer(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// FullMeshLinked constructs a Mocknet with full mesh of Links.
// This means that all the peers **can** connect to each other
// (not that they already are connected. you can use m.ConnectAll())
func FullMeshLinked(ctx context.Context, n int) (Mocknet, error) {
	m, err := WithNPeers(ctx, n)
	if err != nil {
		return nil, err
	}

	if err := m.LinkAll(); err != nil {
		return nil, err
	}

	return m, nil
}

// FullMeshConnected constructs a Mocknet with full mesh of Connections.
// This means that all the peers have dialed and are ready to talk to
// each other.
func FullMeshConnected(ctx context.Context, n int) (Mocknet, error) {
	m, err := FullMeshLinked(ctx, n)
	if err != nil {
		return nil, err
	}

	nets := m.Nets()
	for _, n1 := range nets {
		for _, n2 := range nets {
			if _, err := m.ConnectNets(n1, n2); err != nil {
				return nil, err
			}
		}
	}

	return m, nil
}
