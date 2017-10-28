package relay

import (
	"fmt"
	"net"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	tpt "gx/ipfs/QmQVm7pWYKPStMeMrXNRpvAJE5rSm9ThtQoNmjNHC7sh3k/go-libp2p-transport"
	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ic "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

type Conn struct {
	inet.Stream
	remote    pstore.PeerInfo
	transport tpt.Transport
}

var _ iconn.Conn = (*Conn)(nil)

type NetAddr struct {
	Relay  string
	Remote string
}

func (n *NetAddr) Network() string {
	return "libp2p-circuit-relay"
}

func (n *NetAddr) String() string {
	return fmt.Sprintf("relay[%s-%s]", n.Remote, n.Relay)
}

func (c *Conn) RemoteAddr() net.Addr {
	return &NetAddr{
		Relay:  c.Conn().RemotePeer().Pretty(),
		Remote: c.remote.ID.Pretty(),
	}
}

func (c *Conn) RemoteMultiaddr() ma.Multiaddr {
	a, err := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s/p2p-circuit/ipfs/%s", c.Conn().RemotePeer().Pretty(), c.remote.ID.Pretty()))
	if err != nil {
		panic(err)
	}
	return a
}

func (c *Conn) LocalMultiaddr() ma.Multiaddr {
	return c.Conn().LocalMultiaddr()
}

func (c *Conn) LocalAddr() net.Addr {
	na, err := manet.ToNetAddr(c.Conn().LocalMultiaddr())
	if err != nil {
		log.Error("failed to convert local multiaddr to net addr:", err)
		return nil
	}
	return na
}

func (c *Conn) Transport() tpt.Transport {
	return c.transport
}

func (c *Conn) LocalPeer() peer.ID {
	return c.Conn().LocalPeer()
}

func (c *Conn) RemotePeer() peer.ID {
	return c.remote.ID
}

func (c *Conn) LocalPrivateKey() ic.PrivKey {
	return nil
}

func (c *Conn) RemotePublicKey() ic.PubKey {
	return nil
}

func (c *Conn) ID() string {
	return iconn.ID(c)
}
