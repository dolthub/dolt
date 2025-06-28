package tree

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"golang.org/x/sync/errgroup"
)

func testThreeWayMerge[K ~[]byte, O Ordering[K], S message.Serializer](
	ctx context.Context,
	ns NodeStore,
	left, right, base Node,
	collide CollisionFn,
	leftSchemaChange, rightSchemaChange bool,
	order O,
	serializer S,
) (final Node, stats MergeStats, err error) {
	ld, err := RangeDifferFromRoots[K](ctx, ns, ns, base, left, order, leftSchemaChange)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	rd, err := RangeDifferFromRoots[K](ctx, ns, ns, base, right, order, rightSchemaChange)
	if err != nil {
		return Node{}, MergeStats{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := NewPatchBuffer(PatchBufferSize)

	// iterate |ld| and |rd| in parallel, populating |patches|
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		stats, err = SendPatches(ctx, ld, rd, patches, collide)
		return
	})

	// consume |patches| and apply them to |left|
	eg.Go(func() error {
		final, err = ApplyMutations[K](ctx, ns, left, order, serializer, patches)
		return err
	})

	if err = eg.Wait(); err != nil {
		return Node{}, MergeStats{}, err
	}

	return final, stats, nil
}
