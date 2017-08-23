package namesys

import (
	"context"
	"errors"
	"net"
	"strings"

	path "github.com/ipfs/go-ipfs/path"

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
func NewDNSResolver() Resolver {
	return &DNSResolver{lookupTXT: net.LookupTXT}
}

// newDNSResolver constructs a name resolver using DNS TXT records,
// returning a resolver instead of NewDNSResolver's Resolver.
func newDNSResolver() resolver {
	return &DNSResolver{lookupTXT: net.LookupTXT}
}

// Resolve implements Resolver.
func (r *DNSResolver) Resolve(ctx context.Context, name string) (path.Path, error) {
	return r.ResolveN(ctx, name, DefaultDepthLimit)
}

// ResolveN implements Resolver.
func (r *DNSResolver) ResolveN(ctx context.Context, name string, depth int) (path.Path, error) {
	return resolve(ctx, r, name, depth, "/ipns/")
}

type lookupRes struct {
	path  path.Path
	error error
}

// resolveOnce implements resolver.
// TXT records for a given domain name should contain a b58
// encoded multihash.
func (r *DNSResolver) resolveOnce(ctx context.Context, name string) (path.Path, error) {
	segments := strings.SplitN(name, "/", 2)
	domain := segments[0]

	if !isd.IsDomain(domain) {
		return "", errors.New("not a valid domain name")
	}
	log.Infof("DNSResolver resolving %s", domain)

	rootChan := make(chan lookupRes, 1)
	go workDomain(r, domain, rootChan)

	subChan := make(chan lookupRes, 1)
	go workDomain(r, "_dnslink."+domain, subChan)

	var subRes lookupRes
	select {
	case subRes = <-subChan:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	var p path.Path
	if subRes.error == nil {
		p = subRes.path
	} else {
		var rootRes lookupRes
		select {
		case rootRes = <-rootChan:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		if rootRes.error == nil {
			p = rootRes.path
		} else {
			return "", ErrResolveFailed
		}
	}
	if len(segments) > 1 {
		return path.FromSegments("", strings.TrimRight(p.String(), "/"), segments[1])
	} else {
		return p, nil
	}
}

func workDomain(r *DNSResolver, name string, res chan lookupRes) {
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
