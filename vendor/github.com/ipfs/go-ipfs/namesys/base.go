package namesys

import (
	"strings"

	context "context"

	path "github.com/ipfs/go-ipfs/path"
)

type resolver interface {
	// resolveOnce looks up a name once (without recursion).
	resolveOnce(ctx context.Context, name string) (value path.Path, err error)
}

// resolve is a helper for implementing Resolver.ResolveN using resolveOnce.
func resolve(ctx context.Context, r resolver, name string, depth int, prefixes ...string) (path.Path, error) {
	for {
		p, err := r.resolveOnce(ctx, name)
		if err != nil {
			log.Warningf("Could not resolve %s", name)
			return "", err
		}
		log.Debugf("Resolved %s to %s", name, p.String())

		if strings.HasPrefix(p.String(), "/ipfs/") {
			// we've bottomed out with an IPFS path
			return p, nil
		}

		if depth == 1 {
			return p, ErrResolveRecursion
		}

		matched := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(p.String(), prefix) {
				matched = true
				if len(prefixes) == 1 {
					name = strings.TrimPrefix(p.String(), prefix)
				}
				break
			}
		}

		if !matched {
			return p, nil
		}

		if depth > 1 {
			depth--
		}
	}
}
