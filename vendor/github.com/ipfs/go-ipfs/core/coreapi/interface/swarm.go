package iface

import (
	"context"
	"errors"
	"time"

	ma "gx/ipfs/QmT4U94DnD8FRfqr21obWY32HLM5VExccPKMjQHofeYqr9/go-multiaddr"
	"gx/ipfs/QmTRhk7cgjUf2gfQ3p2M9KPECNZEW9XUrmHcFCgog4cPgB/go-libp2p-peer"
	pstore "gx/ipfs/QmTTJcDL3gsnGDALjh2fDGg1onGRUdVgNL2hU2WEZcVrMX/go-libp2p-peerstore"
	net "gx/ipfs/QmXuRkCR7BNQa9uqfpTiFWsTQLzmTWYg91Ja1w95gnqb6u/go-libp2p-net"
	"gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
)

var (
	ErrNotConnected = errors.New("not connected")
	ErrConnNotFound = errors.New("conn not found")
)

// ConnectionInfo contains information about a peer
type ConnectionInfo interface {
	// ID returns PeerID
	ID() peer.ID

	// Address returns the multiaddress via which we are connected with the peer
	Address() ma.Multiaddr

	// Direction returns which way the connection was established
	Direction() net.Direction

	// Latency returns last known round trip time to the peer
	Latency() (time.Duration, error)

	// Streams returns list of streams established with the peer
	Streams() ([]protocol.ID, error)
}

// SwarmAPI specifies the interface to libp2p swarm
type SwarmAPI interface {
	// Connect to a given peer
	Connect(context.Context, pstore.PeerInfo) error

	// Disconnect from a given address
	Disconnect(context.Context, ma.Multiaddr) error

	// Peers returns the list of peers we are connected to
	Peers(context.Context) ([]ConnectionInfo, error)

	// KnownAddrs returns the list of all addresses this node is aware of
	KnownAddrs(context.Context) (map[peer.ID][]ma.Multiaddr, error)

	// LocalAddrs returns the list of announced listening addresses
	LocalAddrs(context.Context) ([]ma.Multiaddr, error)

	// ListenAddrs returns the list of all listening addresses
	ListenAddrs(context.Context) ([]ma.Multiaddr, error)
}
