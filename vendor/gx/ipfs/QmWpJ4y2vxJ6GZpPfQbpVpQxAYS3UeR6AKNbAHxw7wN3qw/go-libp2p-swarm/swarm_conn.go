package swarm

import (
	"context"
	"fmt"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	ps "gx/ipfs/QmTMNkpso2WRMevXC8ZxgyBhJvoEHvk24SNeUr9Mf9UM1a/go-peerstream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ic "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

// Conn is a simple wrapper around a ps.Conn that also exposes
// some of the methods from the underlying conn.Conn.
// There's **five** "layers" to each connection:
//  * 0. the net.Conn - underlying net.Conn (TCP/UDP/UTP/etc)
//  * 1. the manet.Conn - provides multiaddr friendly Conn
//  * 2. the conn.Conn - provides Peer friendly Conn (inc Secure channel)
//  * 3. the peerstream.Conn - provides peerstream / spdysptream happiness
//  * 4. the Conn - abstracts everyting out, exposing only key parts of underlying layers
// (I know, this is kinda crazy. it's more historical than a good design. though the
// layers do build up pieces of functionality. and they're all just io.RW :) )
type Conn ps.Conn

// ConnHandler is called when new conns are opened from remote peers.
// See peerstream.ConnHandler
type ConnHandler func(*Conn)

func (c *Conn) StreamConn() *ps.Conn {
	return (*ps.Conn)(c)
}

func (c *Conn) RawConn() iconn.Conn {
	// righly panic if these things aren't true. it is an expected
	// invariant that these Conns are all of the typewe expect:
	// 		ps.Conn wrapping a conn.Conn
	// if we get something else it is programmer error.
	return (*ps.Conn)(c).NetConn().(iconn.Conn)
}

func (c *Conn) String() string {
	return fmt.Sprintf("<SwarmConn %s>", c.RawConn())
}

// LocalMultiaddr is the Multiaddr on this side
func (c *Conn) LocalMultiaddr() ma.Multiaddr {
	return c.RawConn().LocalMultiaddr()
}

// LocalPeer is the Peer on our side of the connection
func (c *Conn) LocalPeer() peer.ID {
	return c.RawConn().LocalPeer()
}

// RemoteMultiaddr is the Multiaddr on the remote side
func (c *Conn) RemoteMultiaddr() ma.Multiaddr {
	return c.RawConn().RemoteMultiaddr()
}

// RemotePeer is the Peer on the remote side
func (c *Conn) RemotePeer() peer.ID {
	return c.RawConn().RemotePeer()
}

// LocalPrivateKey is the public key of the peer on this side
func (c *Conn) LocalPrivateKey() ic.PrivKey {
	return c.RawConn().LocalPrivateKey()
}

// RemotePublicKey is the public key of the peer on the remote side
func (c *Conn) RemotePublicKey() ic.PubKey {
	return c.RawConn().RemotePublicKey()
}

// NewSwarmStream returns a new Stream from this connection
func (c *Conn) NewSwarmStream() (*Stream, error) {
	s, err := c.StreamConn().NewStream()
	return (*Stream)(s), err
}

// NewStream returns a new Stream from this connection
func (c *Conn) NewStream() (inet.Stream, error) {
	s, err := c.NewSwarmStream()
	return inet.Stream(s), err
}

// Close closes the underlying stream connection
func (c *Conn) Close() error {
	return c.StreamConn().Close()
}

func (c *Conn) GetStreams() ([]inet.Stream, error) {
	ss := c.StreamConn().Streams()
	out := make([]inet.Stream, len(ss))

	for i, s := range ss {
		out[i] = (*Stream)(s)
	}
	return out, nil
}

func wrapConn(psc *ps.Conn) (*Conn, error) {
	// grab the underlying connection.
	if _, ok := psc.NetConn().(iconn.Conn); !ok {
		// this should never happen. if we see it ocurring it means that we added
		// a Listener to the ps.Swarm that is NOT one of our net/conn.Listener.
		return nil, fmt.Errorf("swarm connHandler: invalid conn (not a conn.Conn): %s", psc)
	}
	return (*Conn)(psc), nil
}

// wrapConns returns a *Conn for all these ps.Conns
func wrapConns(conns1 []*ps.Conn) []*Conn {
	conns2 := make([]*Conn, len(conns1))
	for i, c1 := range conns1 {
		if c2, err := wrapConn(c1); err == nil {
			conns2[i] = c2
		}
	}
	return conns2
}

// newConnSetup does the swarm's "setup" for a connection. returns the underlying
// conn.Conn this method is used by both swarm.Dial and ps.Swarm connHandler
func (s *Swarm) newConnSetup(ctx context.Context, psConn *ps.Conn) (*Conn, error) {

	// wrap with a Conn
	sc, err := wrapConn(psConn)
	if err != nil {
		return nil, err
	}

	// if we have a public key, make sure we add it to our peerstore!
	// This is an important detail. Otherwise we must fetch the public
	// key from the DHT or some other system.
	if pk := sc.RemotePublicKey(); pk != nil {
		s.peers.AddPubKey(sc.RemotePeer(), pk)
	}

	// ok great! we can use it. add it to our group.

	// set the RemotePeer as a group on the conn. this lets us group
	// connections in the StreamSwarm by peer, and get a streams from
	// any available connection in the group (better multiconn):
	//   swarm.StreamSwarm().NewStreamWithGroup(remotePeer)
	psConn.AddGroup(sc.RemotePeer())

	return sc, nil
}
