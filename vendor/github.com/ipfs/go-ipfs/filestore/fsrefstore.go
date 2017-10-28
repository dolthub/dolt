package filestore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-ipfs/blocks/blockstore"
	pb "github.com/ipfs/go-ipfs/filestore/pb"
	dshelp "github.com/ipfs/go-ipfs/thirdparty/ds-help"
	posinfo "github.com/ipfs/go-ipfs/thirdparty/posinfo"
	"gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	proto "gx/ipfs/QmT6n4mspWYEya864BhCUJEgyxiRfmiSY9ruQwTUNpRKaM/protobuf/proto"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsns "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/namespace"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// FilestorePrefix identifies the key prefix for FileManager blocks.
var FilestorePrefix = ds.NewKey("filestore")

// FileManager is a blockstore implementation which stores special
// blocks FilestoreNode type. These nodes only contain a reference
// to the actual location of the block data in the filesystem
// (a path and an offset).
type FileManager struct {
	ds   ds.Batching
	root string
}

// CorruptReferenceError implements the error interface.
// It is used to indicate that the block contents pointed
// by the referencing blocks cannot be retrieved (i.e. the
// file is not found, or the data changed as it was being read).
type CorruptReferenceError struct {
	Code Status
	Err  error
}

// Error() returns the error message in the CorruptReferenceError
// as a string.
func (c CorruptReferenceError) Error() string {
	return c.Err.Error()
}

// NewFileManager initializes a new file manager with the given
// datastore and root. All FilestoreNodes paths are relative to the
// root path given here, which is prepended for any operations.
func NewFileManager(ds ds.Batching, root string) *FileManager {
	return &FileManager{dsns.Wrap(ds, FilestorePrefix), root}
}

// AllKeysChan returns a channel from which to read the keys stored in
// the FileManager. If the given context is cancelled the channel will be
// closed.
func (f *FileManager) AllKeysChan(ctx context.Context) (<-chan *cid.Cid, error) {
	q := dsq.Query{KeysOnly: true}

	res, err := f.ds.Query(q)
	if err != nil {
		return nil, err
	}

	out := make(chan *cid.Cid, dsq.KeysOnlyBufSize)
	go func() {
		defer close(out)
		for {
			v, ok := res.NextSync()
			if !ok {
				return
			}

			k := ds.RawKey(v.Key)
			c, err := dshelp.DsKeyToCid(k)
			if err != nil {
				log.Error("decoding cid from filestore: %s", err)
				continue
			}

			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// DeleteBlock deletes the reference-block from the underlying
// datastore. It does not touch the referenced data.
func (f *FileManager) DeleteBlock(c *cid.Cid) error {
	err := f.ds.Delete(dshelp.CidToDsKey(c))
	if err == ds.ErrNotFound {
		return blockstore.ErrNotFound
	}
	return err
}

// Get reads a block from the datastore. Reading a block
// is done in two steps: the first step retrieves the reference
// block from the datastore. The second step uses the stored
// path and offsets to read the raw block data directly from disk.
func (f *FileManager) Get(c *cid.Cid) (blocks.Block, error) {
	dobj, err := f.getDataObj(c)
	if err != nil {
		return nil, err
	}

	out, err := f.readDataObj(c, dobj)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(out, c)
}

func (f *FileManager) getDataObj(c *cid.Cid) (*pb.DataObj, error) {
	o, err := f.ds.Get(dshelp.CidToDsKey(c))
	switch err {
	case ds.ErrNotFound:
		return nil, blockstore.ErrNotFound
	default:
		return nil, err
	case nil:
		//
	}

	return unmarshalDataObj(o)
}

func unmarshalDataObj(o interface{}) (*pb.DataObj, error) {
	data, ok := o.([]byte)
	if !ok {
		return nil, fmt.Errorf("stored filestore dataobj was not a []byte")
	}

	var dobj pb.DataObj
	if err := proto.Unmarshal(data, &dobj); err != nil {
		return nil, err
	}

	return &dobj, nil
}

// reads and verifies the block
func (f *FileManager) readDataObj(c *cid.Cid, d *pb.DataObj) ([]byte, error) {
	p := filepath.FromSlash(d.GetFilePath())
	abspath := filepath.Join(f.root, p)

	fi, err := os.Open(abspath)
	if os.IsNotExist(err) {
		return nil, &CorruptReferenceError{StatusFileNotFound, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}
	defer fi.Close()

	_, err = fi.Seek(int64(d.GetOffset()), io.SeekStart)
	if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}

	outbuf := make([]byte, d.GetSize_())
	_, err = io.ReadFull(fi, outbuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, &CorruptReferenceError{StatusFileChanged, err}
	} else if err != nil {
		return nil, &CorruptReferenceError{StatusFileError, err}
	}

	outcid, err := c.Prefix().Sum(outbuf)
	if err != nil {
		return nil, err
	}

	if !c.Equals(outcid) {
		return nil, &CorruptReferenceError{StatusFileChanged,
			fmt.Errorf("data in file did not match. %s offset %d", d.GetFilePath(), d.GetOffset())}
	}

	return outbuf, nil
}

// Has returns if the FileManager is storing a block reference. It does not
// validate the data, nor checks if the reference is valid.
func (f *FileManager) Has(c *cid.Cid) (bool, error) {
	// NOTE: interesting thing to consider. Has doesnt validate the data.
	// So the data on disk could be invalid, and we could think we have it.
	dsk := dshelp.CidToDsKey(c)
	return f.ds.Has(dsk)
}

type putter interface {
	Put(ds.Key, interface{}) error
}

// Put adds a new reference block to the FileManager. It does not check
// that the reference is valid.
func (f *FileManager) Put(b *posinfo.FilestoreNode) error {
	return f.putTo(b, f.ds)
}

func (f *FileManager) putTo(b *posinfo.FilestoreNode, to putter) error {
	var dobj pb.DataObj

	if !filepath.HasPrefix(b.PosInfo.FullPath, f.root) {
		return fmt.Errorf("cannot add filestore references outside ipfs root (%s)", f.root)
	}

	p, err := filepath.Rel(f.root, b.PosInfo.FullPath)
	if err != nil {
		return err
	}

	dobj.FilePath = proto.String(filepath.ToSlash(p))
	dobj.Offset = proto.Uint64(b.PosInfo.Offset)
	dobj.Size_ = proto.Uint64(uint64(len(b.RawData())))

	data, err := proto.Marshal(&dobj)
	if err != nil {
		return err
	}

	return to.Put(dshelp.CidToDsKey(b.Cid()), data)
}

// PutMany is like Put() but takes a slice of blocks instead,
// allowing it to create a batch transaction.
func (f *FileManager) PutMany(bs []*posinfo.FilestoreNode) error {
	batch, err := f.ds.Batch()
	if err != nil {
		return err
	}

	for _, b := range bs {
		if err := f.putTo(b, batch); err != nil {
			return err
		}
	}

	return batch.Commit()
}
