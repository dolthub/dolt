package bitswap

import (
	"context"
	"fmt"
	"testing"
	"time"

	blocksutil "github.com/ipfs/go-ipfs/blocks/blocksutil"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
)

func TestBasicSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	block := bgen.Next()
	inst := sesgen.Instances(2)

	a := inst[0]
	b := inst[1]

	if err := b.Blockstore().Put(block); err != nil {
		t.Fatal(err)
	}

	sesa := a.Exchange.NewSession(ctx)

	blkout, err := sesa.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal(err)
	}

	if !blkout.Cid().Equals(block.Cid()) {
		t.Fatal("got wrong block")
	}
}

func assertBlockLists(got, exp []blocks.Block) error {
	if len(got) != len(exp) {
		return fmt.Errorf("got wrong number of blocks, %d != %d", len(got), len(exp))
	}

	h := cid.NewSet()
	for _, b := range got {
		h.Add(b.Cid())
	}
	for _, b := range exp {
		if !h.Has(b.Cid()) {
			return fmt.Errorf("didnt have: %s", b.Cid())
		}
	}
	return nil
}

func TestSessionBetweenPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	inst := sesgen.Instances(10)

	blks := bgen.Blocks(101)
	if err := inst[0].Blockstore().PutMany(blks); err != nil {
		t.Fatal(err)
	}

	var cids []*cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	ses := inst[1].Exchange.NewSession(ctx)
	if _, err := ses.GetBlock(ctx, cids[0]); err != nil {
		t.Fatal(err)
	}
	blks = blks[1:]
	cids = cids[1:]

	for i := 0; i < 10; i++ {
		ch, err := ses.GetBlocks(ctx, cids[i*10:(i+1)*10])
		if err != nil {
			t.Fatal(err)
		}

		var got []blocks.Block
		for b := range ch {
			got = append(got, b)
		}
		if err := assertBlockLists(got, blks[i*10:(i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}
	for _, is := range inst[2:] {
		if is.Exchange.counters.messagesRecvd > 2 {
			t.Fatal("uninvolved nodes should only receive two messages", is.Exchange.counters.messagesRecvd)
		}
	}
}

func TestSessionSplitFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	inst := sesgen.Instances(11)

	blks := bgen.Blocks(100)
	for i := 0; i < 10; i++ {
		if err := inst[i].Blockstore().PutMany(blks[i*10 : (i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}

	var cids []*cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	ses := inst[10].Exchange.NewSession(ctx)
	ses.baseTickDelay = time.Millisecond * 10

	for i := 0; i < 10; i++ {
		ch, err := ses.GetBlocks(ctx, cids[i*10:(i+1)*10])
		if err != nil {
			t.Fatal(err)
		}

		var got []blocks.Block
		for b := range ch {
			got = append(got, b)
		}
		if err := assertBlockLists(got, blks[i*10:(i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInterestCacheOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	blks := bgen.Blocks(2049)
	inst := sesgen.Instances(2)

	a := inst[0]
	b := inst[1]

	ses := a.Exchange.NewSession(ctx)
	zeroch, err := ses.GetBlocks(ctx, []*cid.Cid{blks[0].Cid()})
	if err != nil {
		t.Fatal(err)
	}

	var restcids []*cid.Cid
	for _, blk := range blks[1:] {
		restcids = append(restcids, blk.Cid())
	}

	restch, err := ses.GetBlocks(ctx, restcids)
	if err != nil {
		t.Fatal(err)
	}

	// wait to ensure that all the above cids were added to the sessions cache
	time.Sleep(time.Millisecond * 50)

	if err := b.Exchange.HasBlock(blks[0]); err != nil {
		t.Fatal(err)
	}

	select {
	case blk, ok := <-zeroch:
		if ok && blk.Cid().Equals(blks[0].Cid()) {
			// success!
		} else {
			t.Fatal("failed to get the block")
		}
	case <-restch:
		t.Fatal("should not get anything on restch")
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for block")
	}
}

func TestPutAfterSessionCacheEvict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	blks := bgen.Blocks(2500)
	inst := sesgen.Instances(1)

	a := inst[0]

	ses := a.Exchange.NewSession(ctx)

	var allcids []*cid.Cid
	for _, blk := range blks[1:] {
		allcids = append(allcids, blk.Cid())
	}

	blkch, err := ses.GetBlocks(ctx, allcids)
	if err != nil {
		t.Fatal(err)
	}

	// wait to ensure that all the above cids were added to the sessions cache
	time.Sleep(time.Millisecond * 50)

	if err := a.Exchange.HasBlock(blks[17]); err != nil {
		t.Fatal(err)
	}

	select {
	case <-blkch:
	case <-time.After(time.Millisecond * 50):
		t.Fatal("timed out waiting for block")
	}
}

func TestMultipleSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	sesgen := NewTestSessionGenerator(vnet)
	defer sesgen.Close()
	bgen := blocksutil.NewBlockGenerator()

	blk := bgen.Blocks(1)[0]
	inst := sesgen.Instances(2)

	a := inst[0]
	b := inst[1]

	ctx1, cancel1 := context.WithCancel(ctx)
	ses := a.Exchange.NewSession(ctx1)

	blkch, err := ses.GetBlocks(ctx, []*cid.Cid{blk.Cid()})
	if err != nil {
		t.Fatal(err)
	}
	cancel1()

	ses2 := a.Exchange.NewSession(ctx)
	blkch2, err := ses2.GetBlocks(ctx, []*cid.Cid{blk.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)
	if err := b.Exchange.HasBlock(blk); err != nil {
		t.Fatal(err)
	}

	select {
	case <-blkch2:
	case <-time.After(time.Second * 20):
		t.Fatal("bad juju")
	}
	_ = blkch
}
