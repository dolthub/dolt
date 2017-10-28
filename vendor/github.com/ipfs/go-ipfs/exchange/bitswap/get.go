package bitswap

import (
	"context"
	"errors"

	blockstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	notifications "github.com/ipfs/go-ipfs/exchange/bitswap/notifications"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
)

type getBlocksFunc func(context.Context, []*cid.Cid) (<-chan blocks.Block, error)

func getBlock(p context.Context, k *cid.Cid, gb getBlocksFunc) (blocks.Block, error) {
	if k == nil {
		log.Error("nil cid in GetBlock")
		return nil, blockstore.ErrNotFound
	}

	// Any async work initiated by this function must end when this function
	// returns. To ensure this, derive a new context. Note that it is okay to
	// listen on parent in this scope, but NOT okay to pass |parent| to
	// functions called by this one. Otherwise those functions won't return
	// when this context's cancel func is executed. This is difficult to
	// enforce. May this comment keep you safe.
	ctx, cancel := context.WithCancel(p)
	defer cancel()

	promise, err := gb(ctx, []*cid.Cid{k})
	if err != nil {
		return nil, err
	}

	select {
	case block, ok := <-promise:
		if !ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, errors.New("promise channel was closed")
			}
		}
		return block, nil
	case <-p.Done():
		return nil, p.Err()
	}
}

type wantFunc func(context.Context, []*cid.Cid)

func getBlocksImpl(ctx context.Context, keys []*cid.Cid, notif notifications.PubSub, want wantFunc, cwants func([]*cid.Cid)) (<-chan blocks.Block, error) {
	if len(keys) == 0 {
		out := make(chan blocks.Block)
		close(out)
		return out, nil
	}

	remaining := cid.NewSet()
	promise := notif.Subscribe(ctx, keys...)
	for _, k := range keys {
		log.Event(ctx, "Bitswap.GetBlockRequest.Start", k)
		remaining.Add(k)
	}

	want(ctx, keys)

	out := make(chan blocks.Block)
	go handleIncoming(ctx, remaining, promise, out, cwants)
	return out, nil
}

func handleIncoming(ctx context.Context, remaining *cid.Set, in <-chan blocks.Block, out chan blocks.Block, cfun func([]*cid.Cid)) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		close(out)
		// can't just defer this call on its own, arguments are resolved *when* the defer is created
		cfun(remaining.Keys())
	}()
	for {
		select {
		case blk, ok := <-in:
			if !ok {
				return
			}

			remaining.Remove(blk.Cid())
			select {
			case out <- blk:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
