/*
Package namesys implements resolvers and publishers for the IPFS
naming system (IPNS).

The core of IPFS is an immutable, content-addressable Merkle graph.
That works well for many use cases, but doesn't allow you to answer
questions like "what is Alice's current homepage?".  The mutable name
system allows Alice to publish information like:

  The current homepage for alice.example.com is
  /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj

or:

  The current homepage for node
  QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
  is
  /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj

The mutable name system also allows users to resolve those references
to find the immutable IPFS object currently referenced by a given
mutable name.

For command-line bindings to this functionality, see:

  ipfs name
  ipfs dns
  ipfs resolve
*/
package namesys

import (
	"errors"
	"time"

	context "context"
	path "github.com/ipfs/go-ipfs/path"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

const (
	// DefaultDepthLimit is the default depth limit used by Resolve.
	DefaultDepthLimit = 32

	// UnlimitedDepth allows infinite recursion in ResolveN.  You
	// probably don't want to use this, but it's here if you absolutely
	// trust resolution to eventually complete and can't put an upper
	// limit on how many steps it will take.
	UnlimitedDepth = 0
)

// ErrResolveFailed signals an error when attempting to resolve.
var ErrResolveFailed = errors.New("Could not resolve name.")

// ErrResolveRecursion signals a recursion-depth limit.
var ErrResolveRecursion = errors.New(
	"Could not resolve name (recursion limit exceeded).")

// ErrPublishFailed signals an error when attempting to publish.
var ErrPublishFailed = errors.New("Could not publish name.")

// Namesys represents a cohesive name publishing and resolving system.
//
// Publishing a name is the process of establishing a mapping, a key-value
// pair, according to naming rules and databases.
//
// Resolving a name is the process of looking up the value associated with the
// key (name).
type NameSystem interface {
	Resolver
	Publisher
}

// Resolver is an object capable of resolving names.
type Resolver interface {

	// Resolve performs a recursive lookup, returning the dereferenced
	// path.  For example, if ipfs.io has a DNS TXT record pointing to
	//   /ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
	// and there is a DHT IPNS entry for
	//   QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
	//   -> /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj
	// then
	//   Resolve(ctx, "/ipns/ipfs.io")
	// will resolve both names, returning
	//   /ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj
	//
	// There is a default depth-limit to avoid infinite recursion.  Most
	// users will be fine with this default limit, but if you need to
	// adjust the limit you can use ResolveN.
	Resolve(ctx context.Context, name string) (value path.Path, err error)

	// ResolveN performs a recursive lookup, returning the dereferenced
	// path.  The only difference from Resolve is that the depth limit
	// is configurable.  You can use DefaultDepthLimit, UnlimitedDepth,
	// or a depth limit of your own choosing.
	//
	// Most users should use Resolve, since the default limit works well
	// in most real-world situations.
	ResolveN(ctx context.Context, name string, depth int) (value path.Path, err error)
}

// Publisher is an object capable of publishing particular names.
type Publisher interface {

	// Publish establishes a name-value mapping.
	// TODO make this not PrivKey specific.
	Publish(ctx context.Context, name ci.PrivKey, value path.Path) error

	// TODO: to be replaced by a more generic 'PublishWithValidity' type
	// call once the records spec is implemented
	PublishWithEOL(ctx context.Context, name ci.PrivKey, value path.Path, eol time.Time) error
}
