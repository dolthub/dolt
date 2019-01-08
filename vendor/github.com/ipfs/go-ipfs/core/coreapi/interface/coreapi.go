// Package iface defines IPFS Core API which is a set of interfaces used to
// interact with IPFS nodes.
package iface

import (
	"context"

	ipld "gx/ipfs/QmR7TcHkR9nxkUorfi8XMTAMLUK7GiP64TWWBzY3aacc1o/go-ipld-format"
)

// CoreAPI defines an unified interface to IPFS for Go programs
type CoreAPI interface {
	// Unixfs returns an implementation of Unixfs API
	Unixfs() UnixfsAPI

	// Block returns an implementation of Block API
	Block() BlockAPI

	// Dag returns an implementation of Dag API
	Dag() DagAPI

	// Name returns an implementation of Name API
	Name() NameAPI

	// Key returns an implementation of Key API
	Key() KeyAPI

	// Pin returns an implementation of Pin API
	Pin() PinAPI

	// ObjectAPI returns an implementation of Object API
	Object() ObjectAPI

	// Dht returns an implementation of Dht API
	Dht() DhtAPI

	// Swarm returns an implementation of Swarm API
	Swarm() SwarmAPI

	// PubSub returns an implementation of PubSub API
	PubSub() PubSubAPI

	// ResolvePath resolves the path using Unixfs resolver
	ResolvePath(context.Context, Path) (ResolvedPath, error)

	// ResolveNode resolves the path (if not resolved already) using Unixfs
	// resolver, gets and returns the resolved Node
	ResolveNode(context.Context, Path) (ipld.Node, error)
}
