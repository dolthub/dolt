// Package filestore implements a Blockstore which is able to read certain
// blocks of data directly from its original location in the filesystem.
//
// In a Filestore, object leaves are stored as FilestoreNodes. FilestoreNodes
// include a filesystem path and an offset, allowing a Blockstore dealing with
// such blocks to avoid storing the whole contents and reading them from their
// filesystem location instead.
package filestore

import (
	"context"

	"github.com/ipfs/go-ipfs/blocks/blockstore"
	posinfo "github.com/ipfs/go-ipfs/thirdparty/posinfo"
	"gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

var log = logging.Logger("filestore")

// Filestore implements a Blockstore by combining a standard Blockstore
// to store regular blocks and a special Blockstore called
// FileManager to store blocks which data exists in an external file.
type Filestore struct {
	fm *FileManager
	bs blockstore.Blockstore
}

// FileManager returns the FileManager in Filestore.
func (f *Filestore) FileManager() *FileManager {
	return f.fm
}

// MainBlockstore returns the standard Blockstore in the Filestore.
func (f *Filestore) MainBlockstore() blockstore.Blockstore {
	return f.bs
}

// NewFilestore creates one using the given Blockstore and FileManager.
func NewFilestore(bs blockstore.Blockstore, fm *FileManager) *Filestore {
	return &Filestore{fm, bs}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the blockstore. If the given context is cancelled the channel will be closed.
func (f *Filestore) AllKeysChan(ctx context.Context) (<-chan *cid.Cid, error) {
	ctx, cancel := context.WithCancel(ctx)

	a, err := f.bs.AllKeysChan(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	out := make(chan *cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer cancel()
		defer close(out)

		var done bool
		for !done {
			select {
			case c, ok := <-a:
				if !ok {
					done = true
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}

		// Can't do these at the same time because the abstractions around
		// leveldb make us query leveldb for both operations. We apparently
		// cant query leveldb concurrently
		b, err := f.fm.AllKeysChan(ctx)
		if err != nil {
			log.Error("error querying filestore: ", err)
			return
		}

		done = false
		for !done {
			select {
			case c, ok := <-b:
				if !ok {
					done = true
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// DeleteBlock deletes the block with the given key from the
// blockstore. As expected, in the case of FileManager blocks, only the
// reference is deleted, not its contents. It may return
// ErrNotFound when the block is not stored.
func (f *Filestore) DeleteBlock(c *cid.Cid) error {
	err1 := f.bs.DeleteBlock(c)
	if err1 != nil && err1 != blockstore.ErrNotFound {
		return err1
	}

	err2 := f.fm.DeleteBlock(c)
	// if we successfully removed something from the blockstore, but the
	// filestore didnt have it, return success

	switch err2 {
	case nil:
		return nil
	case blockstore.ErrNotFound:
		if err1 == blockstore.ErrNotFound {
			return blockstore.ErrNotFound
		}
		return nil
	default:
		return err2
	}
}

// Get retrieves the block with the given Cid. It may return
// ErrNotFound when the block is not stored.
func (f *Filestore) Get(c *cid.Cid) (blocks.Block, error) {
	blk, err := f.bs.Get(c)
	switch err {
	default:
		return nil, err
	case nil:
		return blk, nil
	case blockstore.ErrNotFound:
		// try filestore
	}

	return f.fm.Get(c)
}

// Has returns true if the block with the given Cid is
// stored in the Filestore.
func (f *Filestore) Has(c *cid.Cid) (bool, error) {
	has, err := f.bs.Has(c)
	if err != nil {
		return false, err
	}

	if has {
		return true, nil
	}

	return f.fm.Has(c)
}

// Put stores a block in the Filestore. For blocks of
// underlying type FilestoreNode, the operation is
// delegated to the FileManager, while the rest of blocks
// are handled by the regular blockstore.
func (f *Filestore) Put(b blocks.Block) error {
	has, err := f.Has(b.Cid())
	if err != nil {
		return err
	}

	if has {
		return nil
	}

	switch b := b.(type) {
	case *posinfo.FilestoreNode:
		return f.fm.Put(b)
	default:
		return f.bs.Put(b)
	}
}

// PutMany is like Put(), but takes a slice of blocks, allowing
// the underlying blockstore to perform batch transactions.
func (f *Filestore) PutMany(bs []blocks.Block) error {
	var normals []blocks.Block
	var fstores []*posinfo.FilestoreNode

	for _, b := range bs {
		has, err := f.Has(b.Cid())
		if err != nil {
			return err
		}

		if has {
			continue
		}

		switch b := b.(type) {
		case *posinfo.FilestoreNode:
			fstores = append(fstores, b)
		default:
			normals = append(normals, b)
		}
	}

	if len(normals) > 0 {
		err := f.bs.PutMany(normals)
		if err != nil {
			return err
		}
	}

	if len(fstores) > 0 {
		err := f.fm.PutMany(fstores)
		if err != nil {
			return err
		}
	}
	return nil
}

// HashOnRead calls blockstore.HashOnRead.
func (f *Filestore) HashOnRead(enabled bool) {
	f.bs.HashOnRead(enabled)
}

var _ blockstore.Blockstore = (*Filestore)(nil)
