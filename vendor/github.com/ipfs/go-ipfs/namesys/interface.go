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

	opts "github.com/ipfs/go-ipfs/namesys/opts"
	path "gx/ipfs/QmT3rzed1ppXefourpmoZ7tyVQfsGPQZ1pHDngLmCvXxd3/go-path"

	ci "gx/ipfs/QmPvyPwuCgJ7pDmrKDxRtsScJgBaM5h4EpRL2qQJsmXf4n/go-libp2p-crypto"
)

// ErrResolveFailed signals an error when attempting to resolve.
var ErrResolveFailed = errors.New("could not resolve name")

// ErrResolveRecursion signals a recursion-depth limit.
var ErrResolveRecursion = errors.New(
	"could not resolve name (recursion limit exceeded)")

// ErrPublishFailed signals an error when attempting to publish.
var ErrPublishFailed = errors.New("could not publish name")

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

// Result is the return type for Resolver.ResolveAsync.
type Result struct {
	Path path.Path
	Err  error
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
	// adjust the limit you can specify it as an option.
	Resolve(ctx context.Context, name string, options ...opts.ResolveOpt) (value path.Path, err error)

	// ResolveAsync performs recursive name lookup, like Resolve, but it returns
	// entries as they are discovered in the DHT. Each returned result is guaranteed
	// to be "better" (which usually means newer) than the previous one.
	ResolveAsync(ctx context.Context, name string, options ...opts.ResolveOpt) <-chan Result
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
