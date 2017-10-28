package namesys

import (
	"context"
	"strings"
	"time"

	path "github.com/ipfs/go-ipfs/path"

	routing "gx/ipfs/QmPR2JzfKd9poHx9XBhzoFeBBC31ZM3W5iUPKJZWyaoZZm/go-libp2p-routing"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

// mpns (a multi-protocol NameSystem) implements generic IPFS naming.
//
// Uses several Resolvers:
// (a) IPFS routing naming: SFS-like PKI names.
// (b) dns domains: resolves using links in DNS TXT records
// (c) proquints: interprets string as the raw byte data.
//
// It can only publish to: (a) IPFS routing naming.
//
type mpns struct {
	resolvers  map[string]resolver
	publishers map[string]Publisher
}

// NewNameSystem will construct the IPFS naming system based on Routing
func NewNameSystem(r routing.ValueStore, ds ds.Datastore, cachesize int) NameSystem {
	return &mpns{
		resolvers: map[string]resolver{
			"dns":      newDNSResolver(),
			"proquint": new(ProquintResolver),
			"dht":      NewRoutingResolver(r, cachesize),
		},
		publishers: map[string]Publisher{
			"/ipns/": NewRoutingPublisher(r, ds),
		},
	}
}

const DefaultResolverCacheTTL = time.Minute

// Resolve implements Resolver.
func (ns *mpns) Resolve(ctx context.Context, name string) (path.Path, error) {
	return ns.ResolveN(ctx, name, DefaultDepthLimit)
}

// ResolveN implements Resolver.
func (ns *mpns) ResolveN(ctx context.Context, name string, depth int) (path.Path, error) {
	if strings.HasPrefix(name, "/ipfs/") {
		return path.ParsePath(name)
	}

	if !strings.HasPrefix(name, "/") {
		return path.ParsePath("/ipfs/" + name)
	}

	return resolve(ctx, ns, name, depth, "/ipns/")
}

// resolveOnce implements resolver.
func (ns *mpns) resolveOnce(ctx context.Context, name string) (path.Path, error) {
	if !strings.HasPrefix(name, "/ipns/") {
		name = "/ipns/" + name
	}
	segments := strings.SplitN(name, "/", 4)
	if len(segments) < 3 || segments[0] != "" {
		log.Warningf("Invalid name syntax for %s", name)
		return "", ErrResolveFailed
	}

	for protocol, resolver := range ns.resolvers {
		log.Debugf("Attempting to resolve %s with %s", segments[2], protocol)
		p, err := resolver.resolveOnce(ctx, segments[2])
		if err == nil {
			if len(segments) > 3 {
				return path.FromSegments("", strings.TrimRight(p.String(), "/"), segments[3])
			} else {
				return p, err
			}
		}
	}
	log.Warningf("No resolver found for %s", name)
	return "", ErrResolveFailed
}

// Publish implements Publisher
func (ns *mpns) Publish(ctx context.Context, name ci.PrivKey, value path.Path) error {
	err := ns.publishers["/ipns/"].Publish(ctx, name, value)
	if err != nil {
		return err
	}
	ns.addToDHTCache(name, value, time.Now().Add(DefaultRecordTTL))
	return nil
}

func (ns *mpns) PublishWithEOL(ctx context.Context, name ci.PrivKey, value path.Path, eol time.Time) error {
	err := ns.publishers["/ipns/"].PublishWithEOL(ctx, name, value, eol)
	if err != nil {
		return err
	}
	ns.addToDHTCache(name, value, eol)
	return nil
}

func (ns *mpns) addToDHTCache(key ci.PrivKey, value path.Path, eol time.Time) {
	rr, ok := ns.resolvers["dht"].(*routingResolver)
	if !ok {
		// should never happen, purely for sanity
		log.Panicf("unexpected type %T as DHT resolver.", ns.resolvers["dht"])
	}
	if rr.cache == nil {
		// resolver has no caching
		return
	}

	var err error
	value, err = path.ParsePath(value.String())
	if err != nil {
		log.Error("could not parse path")
		return
	}

	name, err := peer.IDFromPrivateKey(key)
	if err != nil {
		log.Error("while adding to cache, could not get peerid from private key")
		return
	}

	if time.Now().Add(DefaultResolverCacheTTL).Before(eol) {
		eol = time.Now().Add(DefaultResolverCacheTTL)
	}
	rr.cache.Add(name.Pretty(), cacheEntry{
		val: value,
		eol: eol,
	})
}
