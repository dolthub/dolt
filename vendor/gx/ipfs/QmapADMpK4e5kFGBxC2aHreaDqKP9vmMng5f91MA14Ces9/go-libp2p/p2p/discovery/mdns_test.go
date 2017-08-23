package discovery

import (
	"context"
	"testing"
	"time"

	bhost "gx/ipfs/QmapADMpK4e5kFGBxC2aHreaDqKP9vmMng5f91MA14Ces9/go-libp2p/p2p/host/basic"

	netutil "gx/ipfs/QmViDDJGzv2TKrheoxckReECc72iRgaYsobG2HYUGWuPVF/go-libp2p-netutil"
	host "gx/ipfs/QmZy7c24mmkEHpNJndwgsEE3wcVxHd8yB969yTnAJFVw7f/go-libp2p-host"

	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
)

type DiscoveryNotifee struct {
	h host.Host
}

func (n *DiscoveryNotifee) HandlePeerFound(pi pstore.PeerInfo) {
	n.h.Connect(context.Background(), pi)
}

func TestMdnsDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := bhost.New(netutil.GenSwarmNetwork(t, ctx))
	b := bhost.New(netutil.GenSwarmNetwork(t, ctx))

	sa, err := NewMdnsService(ctx, a, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	sb, err := NewMdnsService(ctx, b, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	_ = sb

	n := &DiscoveryNotifee{a}

	sa.RegisterNotifee(n)

	time.Sleep(time.Second * 2)

	err = a.Connect(ctx, pstore.PeerInfo{ID: b.ID()})
	if err != nil {
		t.Fatal(err)
	}
}
