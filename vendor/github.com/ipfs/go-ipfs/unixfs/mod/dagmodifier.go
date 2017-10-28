package mod

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	chunk "github.com/ipfs/go-ipfs/importer/chunk"
	help "github.com/ipfs/go-ipfs/importer/helpers"
	trickle "github.com/ipfs/go-ipfs/importer/trickle"
	mdag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

var ErrSeekFail = errors.New("failed to seek properly")
var ErrUnrecognizedWhence = errors.New("unrecognized whence")

// 2MB
var writebufferSize = 1 << 21

// DagModifier is the only struct licensed and able to correctly
// perform surgery on a DAG 'file'
// Dear god, please rename this to something more pleasant
type DagModifier struct {
	dagserv mdag.DAGService
	curNode node.Node

	splitter   chunk.SplitterGen
	ctx        context.Context
	readCancel func()

	writeStart uint64
	curWrOff   uint64
	wrBuf      *bytes.Buffer

	Prefix    cid.Prefix
	RawLeaves bool

	read uio.DagReader
}

var ErrNotUnixfs = fmt.Errorf("dagmodifier only supports unixfs nodes (proto or raw)")

// NewDagModifier returns a new DagModifier, the Cid prefix for newly
// created nodes will be inherted from the passed in node.  If the Cid
// version if not 0 raw leaves will also be enabled.  The Prefix and
// RawLeaves options can be overridden by changing them after the call.
func NewDagModifier(ctx context.Context, from node.Node, serv mdag.DAGService, spl chunk.SplitterGen) (*DagModifier, error) {
	switch from.(type) {
	case *mdag.ProtoNode, *mdag.RawNode:
		// ok
	default:
		return nil, ErrNotUnixfs
	}

	prefix := from.Cid().Prefix()
	prefix.Codec = cid.DagProtobuf
	rawLeaves := false
	if prefix.Version > 0 {
		rawLeaves = true
	}

	return &DagModifier{
		curNode:   from.Copy(),
		dagserv:   serv,
		splitter:  spl,
		ctx:       ctx,
		Prefix:    prefix,
		RawLeaves: rawLeaves,
	}, nil
}

// WriteAt will modify a dag file in place
func (dm *DagModifier) WriteAt(b []byte, offset int64) (int, error) {
	// TODO: this is currently VERY inneficient
	// each write that happens at an offset other than the current one causes a
	// flush to disk, and dag rewrite
	if offset == int64(dm.writeStart) && dm.wrBuf != nil {
		// If we would overwrite the previous write
		if len(b) >= dm.wrBuf.Len() {
			dm.wrBuf.Reset()
		}
	} else if uint64(offset) != dm.curWrOff {
		size, err := dm.Size()
		if err != nil {
			return 0, err
		}
		if offset > size {
			err := dm.expandSparse(offset - size)
			if err != nil {
				return 0, err
			}
		}

		err = dm.Sync()
		if err != nil {
			return 0, err
		}
		dm.writeStart = uint64(offset)
	}

	return dm.Write(b)
}

// A reader that just returns zeros
type zeroReader struct{}

func (zr zeroReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

// expandSparse grows the file with zero blocks of 4096
// A small blocksize is chosen to aid in deduplication
func (dm *DagModifier) expandSparse(size int64) error {
	r := io.LimitReader(zeroReader{}, size)
	spl := chunk.NewSizeSplitter(r, 4096)
	nnode, err := dm.appendData(dm.curNode, spl)
	if err != nil {
		return err
	}
	_, err = dm.dagserv.Add(nnode)
	return err
}

// Write continues writing to the dag at the current offset
func (dm *DagModifier) Write(b []byte) (int, error) {
	if dm.read != nil {
		dm.read = nil
	}
	if dm.wrBuf == nil {
		dm.wrBuf = new(bytes.Buffer)
	}

	n, err := dm.wrBuf.Write(b)
	if err != nil {
		return n, err
	}
	dm.curWrOff += uint64(n)
	if dm.wrBuf.Len() > writebufferSize {
		err := dm.Sync()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Size returns the Filesize of the node
func (dm *DagModifier) Size() (int64, error) {
	fileSize, err := fileSize(dm.curNode)
	if err != nil {
		return 0, err
	}
	if dm.wrBuf != nil && int64(dm.wrBuf.Len())+int64(dm.writeStart) > int64(fileSize) {
		return int64(dm.wrBuf.Len()) + int64(dm.writeStart), nil
	}
	return int64(fileSize), nil
}

func fileSize(n node.Node) (uint64, error) {
	switch nd := n.(type) {
	case *mdag.ProtoNode:
		f, err := ft.FromBytes(nd.Data())
		if err != nil {
			return 0, err
		}
		return f.GetFilesize(), nil
	case *mdag.RawNode:
		return uint64(len(nd.RawData())), nil
	default:
		return 0, ErrNotUnixfs
	}
}

// Sync writes changes to this dag to disk
func (dm *DagModifier) Sync() error {
	// No buffer? Nothing to do
	if dm.wrBuf == nil {
		return nil
	}

	// If we have an active reader, kill it
	if dm.read != nil {
		dm.read = nil
		dm.readCancel()
	}

	// Number of bytes we're going to write
	buflen := dm.wrBuf.Len()

	// overwrite existing dag nodes
	thisc, done, err := dm.modifyDag(dm.curNode, dm.writeStart, dm.wrBuf)
	if err != nil {
		return err
	}

	dm.curNode, err = dm.dagserv.Get(dm.ctx, thisc)
	if err != nil {
		return err
	}

	// need to write past end of current dag
	if !done {
		dm.curNode, err = dm.appendData(dm.curNode, dm.splitter(dm.wrBuf))
		if err != nil {
			return err
		}

		_, err = dm.dagserv.Add(dm.curNode)
		if err != nil {
			return err
		}
	}

	dm.writeStart += uint64(buflen)

	dm.wrBuf = nil
	return nil
}

// modifyDag writes the data in 'data' over the data in 'node' starting at 'offset'
// returns the new key of the passed in node and whether or not all the data in the reader
// has been consumed.
func (dm *DagModifier) modifyDag(n node.Node, offset uint64, data io.Reader) (*cid.Cid, bool, error) {
	// If we've reached a leaf node.
	if len(n.Links()) == 0 {
		switch nd0 := n.(type) {
		case *mdag.ProtoNode:
			f, err := ft.FromBytes(nd0.Data())
			if err != nil {
				return nil, false, err
			}

			n, err := data.Read(f.Data[offset:])
			if err != nil && err != io.EOF {
				return nil, false, err
			}

			// Update newly written node..
			b, err := proto.Marshal(f)
			if err != nil {
				return nil, false, err
			}

			nd := new(mdag.ProtoNode)
			nd.SetData(b)
			nd.SetPrefix(&nd0.Prefix)
			k, err := dm.dagserv.Add(nd)
			if err != nil {
				return nil, false, err
			}

			// Hey look! we're done!
			var done bool
			if n < len(f.Data[offset:]) {
				done = true
			}

			return k, done, nil
		case *mdag.RawNode:
			origData := nd0.RawData()
			bytes := make([]byte, len(origData))

			// copy orig data up to offset
			copy(bytes, origData[:offset])

			// copy in new data
			n, err := data.Read(bytes[offset:])
			if err != nil && err != io.EOF {
				return nil, false, err
			}

			// copy remaining data
			offsetPlusN := int(offset) + n
			if offsetPlusN < len(origData) {
				copy(bytes[offsetPlusN:], origData[offsetPlusN:])
			}

			nd, err := mdag.NewRawNodeWPrefix(bytes, nd0.Cid().Prefix())
			if err != nil {
				return nil, false, err
			}
			k, err := dm.dagserv.Add(nd)
			if err != nil {
				return nil, false, err
			}

			// Hey look! we're done!
			var done bool
			if n < len(bytes[offset:]) {
				done = true
			}

			return k, done, nil
		}
	}

	node, ok := n.(*mdag.ProtoNode)
	if !ok {
		return nil, false, ErrNotUnixfs
	}

	f, err := ft.FromBytes(node.Data())
	if err != nil {
		return nil, false, err
	}

	var cur uint64
	var done bool
	for i, bs := range f.GetBlocksizes() {
		// We found the correct child to write into
		if cur+bs > offset {
			child, err := node.Links()[i].GetNode(dm.ctx, dm.dagserv)
			if err != nil {
				return nil, false, err
			}

			k, sdone, err := dm.modifyDag(child, offset-cur, data)
			if err != nil {
				return nil, false, err
			}

			offset += bs
			node.Links()[i].Cid = k

			// Recache serialized node
			_, err = node.EncodeProtobuf(true)
			if err != nil {
				return nil, false, err
			}

			if sdone {
				// No more bytes to write!
				done = true
				break
			}
			offset = cur + bs
		}
		cur += bs
	}

	k, err := dm.dagserv.Add(node)
	return k, done, err
}

// appendData appends the blocks from the given chan to the end of this dag
func (dm *DagModifier) appendData(nd node.Node, spl chunk.Splitter) (node.Node, error) {
	switch nd := nd.(type) {
	case *mdag.ProtoNode, *mdag.RawNode:
		dbp := &help.DagBuilderParams{
			Dagserv:   dm.dagserv,
			Maxlinks:  help.DefaultLinksPerBlock,
			Prefix:    &dm.Prefix,
			RawLeaves: dm.RawLeaves,
		}
		return trickle.TrickleAppend(dm.ctx, nd, dbp.New(spl))
	default:
		return nil, ErrNotUnixfs
	}
}

// Read data from this dag starting at the current offset
func (dm *DagModifier) Read(b []byte) (int, error) {
	err := dm.readPrep()
	if err != nil {
		return 0, err
	}

	n, err := dm.read.Read(b)
	dm.curWrOff += uint64(n)
	return n, err
}

func (dm *DagModifier) readPrep() error {
	err := dm.Sync()
	if err != nil {
		return err
	}

	if dm.read == nil {
		ctx, cancel := context.WithCancel(dm.ctx)
		dr, err := uio.NewDagReader(ctx, dm.curNode, dm.dagserv)
		if err != nil {
			cancel()
			return err
		}

		i, err := dr.Seek(int64(dm.curWrOff), io.SeekStart)
		if err != nil {
			cancel()
			return err
		}

		if i != int64(dm.curWrOff) {
			cancel()
			return ErrSeekFail
		}

		dm.readCancel = cancel
		dm.read = dr
	}

	return nil
}

// Read data from this dag starting at the current offset
func (dm *DagModifier) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	err := dm.readPrep()
	if err != nil {
		return 0, err
	}

	n, err := dm.read.CtxReadFull(ctx, b)
	dm.curWrOff += uint64(n)
	return n, err
}

// GetNode gets the modified DAG Node
func (dm *DagModifier) GetNode() (node.Node, error) {
	err := dm.Sync()
	if err != nil {
		return nil, err
	}
	return dm.curNode.Copy(), nil
}

// HasChanges returned whether or not there are unflushed changes to this dag
func (dm *DagModifier) HasChanges() bool {
	return dm.wrBuf != nil
}

func (dm *DagModifier) Seek(offset int64, whence int) (int64, error) {
	err := dm.Sync()
	if err != nil {
		return 0, err
	}

	fisize, err := dm.Size()
	if err != nil {
		return 0, err
	}

	var newoffset uint64
	switch whence {
	case io.SeekCurrent:
		newoffset = dm.curWrOff + uint64(offset)
	case io.SeekStart:
		newoffset = uint64(offset)
	case io.SeekEnd:
		newoffset = uint64(fisize) - uint64(offset)
	default:
		return 0, ErrUnrecognizedWhence
	}

	if int64(newoffset) > fisize {
		if err := dm.expandSparse(int64(newoffset) - fisize); err != nil {
			return 0, err
		}
	}
	dm.curWrOff = newoffset
	dm.writeStart = newoffset

	if dm.read != nil {
		_, err = dm.read.Seek(offset, whence)
		if err != nil {
			return 0, err
		}
	}

	return int64(dm.curWrOff), nil
}

func (dm *DagModifier) Truncate(size int64) error {
	err := dm.Sync()
	if err != nil {
		return err
	}

	realSize, err := dm.Size()
	if err != nil {
		return err
	}

	// Truncate can also be used to expand the file
	if size > int64(realSize) {
		return dm.expandSparse(int64(size) - realSize)
	}

	nnode, err := dagTruncate(dm.ctx, dm.curNode, uint64(size), dm.dagserv)
	if err != nil {
		return err
	}

	_, err = dm.dagserv.Add(nnode)
	if err != nil {
		return err
	}

	dm.curNode = nnode
	return nil
}

// dagTruncate truncates the given node to 'size' and returns the modified Node
func dagTruncate(ctx context.Context, n node.Node, size uint64, ds mdag.DAGService) (node.Node, error) {
	if len(n.Links()) == 0 {
		switch nd := n.(type) {
		case *mdag.ProtoNode:
			// TODO: this can likely be done without marshaling and remarshaling
			pbn, err := ft.FromBytes(nd.Data())
			if err != nil {
				return nil, err
			}
			nd.SetData(ft.WrapData(pbn.Data[:size]))
			return nd, nil
		case *mdag.RawNode:
			return mdag.NewRawNodeWPrefix(nd.RawData()[:size], nd.Cid().Prefix())
		}
	}

	nd, ok := n.(*mdag.ProtoNode)
	if !ok {
		return nil, ErrNotUnixfs
	}

	var cur uint64
	end := 0
	var modified node.Node
	ndata := new(ft.FSNode)
	for i, lnk := range nd.Links() {
		child, err := lnk.GetNode(ctx, ds)
		if err != nil {
			return nil, err
		}

		childsize, err := fileSize(child)
		if err != nil {
			return nil, err
		}

		// found the child we want to cut
		if size < cur+childsize {
			nchild, err := dagTruncate(ctx, child, size-cur, ds)
			if err != nil {
				return nil, err
			}

			ndata.AddBlockSize(size - cur)

			modified = nchild
			end = i
			break
		}
		cur += childsize
		ndata.AddBlockSize(childsize)
	}

	_, err := ds.Add(modified)
	if err != nil {
		return nil, err
	}

	nd.SetLinks(nd.Links()[:end])
	err = nd.AddNodeLinkClean("", modified)
	if err != nil {
		return nil, err
	}

	d, err := ndata.GetBytes()
	if err != nil {
		return nil, err
	}

	nd.SetData(d)

	// invalidate cache and recompute serialized data
	_, err = nd.EncodeProtobuf(true)
	if err != nil {
		return nil, err
	}

	return nd, nil
}
