package testutil

import (
	"context"
	"time"
)

// WaitFor waits for `check` to stop returning an error or for the context to be
// canceled (whichever comes first).
func WaitFor(ctx context.Context, check func() error) error {
	for {
		time.Sleep(time.Millisecond * 10)
		err := check()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
	}
}
