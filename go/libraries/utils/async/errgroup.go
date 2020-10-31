package async

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

// GoWithCancel runs a context-aware error-returning function on the provided
// |*errgroup.Group|. It passes the function a child context of the provided
// |ctx| and returns a |func()| to cancel that child context. If the provided
// function returns a |context.Canceled| error, then the |*errgroup.Group| will
// see a return of |ctx.Err()|, instead of the child context's |Err()|.
//
// If the provided function returns or approrpiately wraps a |context.Canceled|
// error that it sees in processing, this function allows for dispatching
// cancelable work on an |*errgroup.Group| and canceling that work, without the
// |*errgroup.Group| itself seeing an |err| and canceling.
func GoWithCancel(ctx context.Context, g *errgroup.Group, f func(context.Context) error) func() {
	sctx, cancel := context.WithCancel(ctx)
	g.Go(func() error {
		err := f(sctx)
		if errors.Is(err, context.Canceled) {
			return ctx.Err()
		}
		return err
	})
	return cancel
}
