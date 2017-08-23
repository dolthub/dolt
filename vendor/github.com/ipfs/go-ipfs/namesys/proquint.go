package namesys

import (
	"errors"

	context "context"
	path "github.com/ipfs/go-ipfs/path"
	proquint "gx/ipfs/QmYnf27kzqR2cxt6LFZdrAFJuQd6785fTkBvMuEj9EeRxM/proquint"
)

type ProquintResolver struct{}

// Resolve implements Resolver.
func (r *ProquintResolver) Resolve(ctx context.Context, name string) (path.Path, error) {
	return r.ResolveN(ctx, name, DefaultDepthLimit)
}

// ResolveN implements Resolver.
func (r *ProquintResolver) ResolveN(ctx context.Context, name string, depth int) (path.Path, error) {
	return resolve(ctx, r, name, depth, "/ipns/")
}

// resolveOnce implements resolver. Decodes the proquint string.
func (r *ProquintResolver) resolveOnce(ctx context.Context, name string) (path.Path, error) {
	ok, err := proquint.IsProquint(name)
	if err != nil || !ok {
		return "", errors.New("not a valid proquint string")
	}
	return path.FromString(string(proquint.Decode(name))), nil
}
