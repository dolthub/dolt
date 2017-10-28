package host

import (
	"context"

	inet "gx/ipfs/QmNa31VPzC561NWwRsJLE7nGYZYuuD2QfpK2b1q9BK54J1/go-libp2p-net"
	pstore "gx/ipfs/QmPgDWmTmuzvP7QE5zwo1TmjbJme9pmZHNujB2453jkCTr/go-libp2p-peerstore"
	msmux "gx/ipfs/QmTnsezaB1wWNRHeHnYrm8K4d5i9wtyj3GsqjC3Rt5b5v5/go-multistream"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ifconnmgr "gx/ipfs/QmYkCrTwivapqdB3JbwvwvxymseahVkcm46ThRMAA24zCr/go-libp2p-interface-connmgr"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
)

// Host is an object participating in a p2p network, which
// implements protocols or provides services. It handles
// requests like a Server, and issues requests like a Client.
// It is called Host because it is both Server and Client (and Peer
// may be confusing).
type Host interface {
	// ID returns the (local) peer.ID associated with this Host
	ID() peer.ID

	// Peerstore returns the Host's repository of Peer Addresses and Keys.
	Peerstore() pstore.Peerstore

	// Returns the listen addresses of the Host
	Addrs() []ma.Multiaddr

	// Networks returns the Network interface of the Host
	Network() inet.Network

	// Mux returns the Mux multiplexing incoming streams to protocol handlers
	Mux() *msmux.MultistreamMuxer

	// Connect ensures there is a connection between this host and the peer with
	// given peer.ID. Connect will absorb the addresses in pi into its internal
	// peerstore. If there is not an active connection, Connect will issue a
	// h.Network.Dial, and block until a connection is open, or an error is
	// returned. // TODO: Relay + NAT.
	Connect(ctx context.Context, pi pstore.PeerInfo) error

	// SetStreamHandler sets the protocol handler on the Host's Mux.
	// This is equivalent to:
	//   host.Mux().SetHandler(proto, handler)
	// (Threadsafe)
	SetStreamHandler(pid protocol.ID, handler inet.StreamHandler)

	// SetStreamHandlerMatch sets the protocol handler on the Host's Mux
	// using a matching function for protocol selection.
	SetStreamHandlerMatch(protocol.ID, func(string) bool, inet.StreamHandler)

	// RemoveStreamHandler removes a handler on the mux that was set by
	// SetStreamHandler
	RemoveStreamHandler(pid protocol.ID)

	// NewStream opens a new stream to given peer p, and writes a p2p/protocol
	// header with given protocol.ID. If there is no connection to p, attempts
	// to create one. If ProtocolID is "", writes no header.
	// (Threadsafe)
	NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (inet.Stream, error)

	// Close shuts down the host, its Network, and services.
	Close() error

	// ConnManager returns this hosts connection manager
	ConnManager() ifconnmgr.ConnManager
}
