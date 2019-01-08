package namesys

import (
	"context"
	"errors"
	"net"
	"strings"

	opts "github.com/ipfs/go-ipfs/namesys/opts"

	path "gx/ipfs/QmT3rzed1ppXefourpmoZ7tyVQfsGPQZ1pHDngLmCvXxd3/go-path"
	isd "gx/ipfs/QmZmmuAXgX73UQmX1jRKjTGmjzq24Jinqkq8vzkBtno4uX/go-is-domain"
)

type LookupTXTFunc func(name string) (txt []string, err error)

// DNSResolver implements a Resolver on DNS domains
type DNSResolver struct {
	lookupTXT LookupTXTFunc
	// TODO: maybe some sort of caching?
	// cache would need a timeout
}

// NewDNSResolver constructs a name resolver using DNS TXT records.
func NewDNSResolver() *DNSResolver {
	return &DNSResolver{lookupTXT: net.LookupTXT}
}

// Resolve implements Resolver.
func (r *DNSResolver) Resolve(ctx context.Context, name string, options ...opts.ResolveOpt) (path.Path, error) {
	return resolve(ctx, r, name, opts.ProcessOpts(options))
}

// ResolveAsync implements Resolver.
func (r *DNSResolver) ResolveAsync(ctx context.Context, name string, options ...opts.ResolveOpt) <-chan Result {
	return resolveAsync(ctx, r, name, opts.ProcessOpts(options))
}

type lookupRes struct {
	path  path.Path
	error error
}

// resolveOnce implements resolver.
// TXT records for a given domain name should contain a b58
// encoded multihash.
func (r *DNSResolver) resolveOnceAsync(ctx context.Context, name string, options opts.ResolveOpts) <-chan onceResult {
	out := make(chan onceResult, 1)
	segments := strings.SplitN(name, "/", 2)
	domain := segments[0]

	if !isd.IsDomain(domain) {
		out <- onceResult{err: errors.New("not a valid domain name")}
		close(out)
		return out
	}
	log.Debugf("DNSResolver resolving %s", domain)

	rootChan := make(chan lookupRes, 1)
	go workDomain(r, domain, rootChan)

	subChan := make(chan lookupRes, 1)
	go workDomain(r, "_dnslink."+domain, subChan)

	appendPath := func(p path.Path) (path.Path, error) {
		if len(segments) > 1 {
			return path.FromSegments("", strings.TrimRight(p.String(), "/"), segments[1])
		}
		return p, nil
	}

	go func() {
		defer close(out)
		for {
			select {
			case subRes, ok := <-subChan:
				if !ok {
					subChan = nil
					break
				}
				if subRes.error == nil {
					p, err := appendPath(subRes.path)
					emitOnceResult(ctx, out, onceResult{value: p, err: err})
					return
				}
			case rootRes, ok := <-rootChan:
				if !ok {
					rootChan = nil
					break
				}
				if rootRes.error == nil {
					p, err := appendPath(rootRes.path)
					emitOnceResult(ctx, out, onceResult{value: p, err: err})
				}
			case <-ctx.Done():
				return
			}
			if subChan == nil && rootChan == nil {
				return
			}
		}
	}()

	return out
}

func workDomain(r *DNSResolver, name string, res chan lookupRes) {
	defer close(res)

	txt, err := r.lookupTXT(name)
	if err != nil {
		// Error is != nil
		res <- lookupRes{"", err}
		return
	}

	for _, t := range txt {
		p, err := parseEntry(t)
		if err == nil {
			res <- lookupRes{p, nil}
			return
		}
	}
	res <- lookupRes{"", ErrResolveFailed}
}

func parseEntry(txt string) (path.Path, error) {
	p, err := path.ParseCidToPath(txt) // bare IPFS multihashes
	if err == nil {
		return p, nil
	}

	return tryParseDnsLink(txt)
}

func tryParseDnsLink(txt string) (path.Path, error) {
	parts := strings.SplitN(txt, "=", 2)
	if len(parts) == 2 && parts[0] == "dnslink" {
		return path.ParsePath(parts[1])
	}

	return "", errors.New("not a valid dnslink entry")
}
