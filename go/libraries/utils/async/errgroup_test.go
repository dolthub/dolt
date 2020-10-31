package async

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestGoWithCancel(t *testing.T) {
	t.Run("NilError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			return nil
		})
		assert.NoError(t, eg.Wait())
	})
	t.Run("NonNilError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			return errors.New("there was an error")
		})
		assert.Error(t, eg.Wait())
	})
	t.Run("CancelNoError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		cancel := GoWithCancel(ctx, eg, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		assert.NoError(t, eg.Wait())
	})
	t.Run("CancelParentError", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		eg, ctx := errgroup.WithContext(parent)
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		assert.Error(t, eg.Wait())
	})
}
