// Copyright 2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
