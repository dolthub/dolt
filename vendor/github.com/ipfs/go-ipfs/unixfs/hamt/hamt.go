// Package hamt implements a Hash Array Mapped Trie over ipfs merkledag nodes.
// It is implemented mostly as described in the wikipedia article on HAMTs,
// however the table size is variable (usually 256 in our usages) as opposed to
// 32 as suggested in the article.  The hash function used is currently
// Murmur3, but this value is configurable (the datastructure reports which
// hash function its using).
//
// The one algorithmic change we implement that is not mentioned in the
// wikipedia article is the collapsing of empty shards.
// Given the following tree: ( '[' = shards, '{' = values )
// [ 'A' ] -> [ 'B' ] -> { "ABC" }
//    |       L-> { "ABD" }
//    L-> { "ASDF" }
// If we simply removed "ABC", we would end up with a tree where shard 'B' only
// has a single child.  This causes two issues, the first, is that now we have
// an extra lookup required to get to "ABD".  The second issue is that now we
// have a tree that contains only "ABD", but is not the same tree that we would
// get by simply inserting "ABD" into a new tree.  To address this, we always
// check for empty shard nodes upon deletion and prune them to maintain a
// consistent tree, independent of insertion order.
package hamt

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"os"

	dag "github.com/ipfs/go-ipfs/merkledag"
	format "github.com/ipfs/go-ipfs/unixfs"
	upb "github.com/ipfs/go-ipfs/unixfs/pb"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	"gx/ipfs/QmfJHywXQu98UeZtGJBQrPAR6AtmDjjbe3qjTo9piXHPnx/murmur3"
)

const (
	HashMurmur3 uint64 = 0x22
)

type HamtShard struct {
	nd *dag.ProtoNode

	bitfield *big.Int

	children []child

	tableSize    int
	tableSizeLg2 int

	prefix   *cid.Prefix
	hashFunc uint64

	prefixPadStr string
	maxpadlen    int

	dserv dag.DAGService
}

// child can either be another shard, or a leaf node value
type child interface {
	Link() (*node.Link, error)
	Label() string
}

func NewHamtShard(dserv dag.DAGService, size int) (*HamtShard, error) {
	ds, err := makeHamtShard(dserv, size)
	if err != nil {
		return nil, err
	}

	ds.bitfield = big.NewInt(0)
	ds.nd = new(dag.ProtoNode)
	ds.hashFunc = HashMurmur3
	return ds, nil
}

func makeHamtShard(ds dag.DAGService, size int) (*HamtShard, error) {
	lg2s := int(math.Log2(float64(size)))
	if 1<<uint(lg2s) != size {
		return nil, fmt.Errorf("hamt size should be a power of two")
	}
	maxpadding := fmt.Sprintf("%X", size-1)
	return &HamtShard{
		tableSizeLg2: lg2s,
		prefixPadStr: fmt.Sprintf("%%0%dX", len(maxpadding)),
		maxpadlen:    len(maxpadding),
		tableSize:    size,
		dserv:        ds,
	}, nil
}

func NewHamtFromDag(dserv dag.DAGService, nd node.Node) (*HamtShard, error) {
	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrLinkNotFound
	}

	pbd, err := format.FromBytes(pbnd.Data())
	if err != nil {
		return nil, err
	}

	if pbd.GetType() != upb.Data_HAMTShard {
		return nil, fmt.Errorf("node was not a dir shard")
	}

	if pbd.GetHashType() != HashMurmur3 {
		return nil, fmt.Errorf("only murmur3 supported as hash function")
	}

	ds, err := makeHamtShard(dserv, int(pbd.GetFanout()))
	if err != nil {
		return nil, err
	}

	ds.nd = pbnd.Copy().(*dag.ProtoNode)
	ds.children = make([]child, len(pbnd.Links()))
	ds.bitfield = new(big.Int).SetBytes(pbd.GetData())
	ds.hashFunc = pbd.GetHashType()
	ds.prefix = &ds.nd.Prefix

	return ds, nil
}

// SetPrefix sets the CID Prefix
func (ds *HamtShard) SetPrefix(prefix *cid.Prefix) {
	ds.prefix = prefix
}

// Prefix gets the CID Prefix, may be nil if unset
func (ds *HamtShard) Prefix() *cid.Prefix {
	return ds.prefix
}

// Node serializes the HAMT structure into a merkledag node with unixfs formatting
func (ds *HamtShard) Node() (node.Node, error) {
	out := new(dag.ProtoNode)
	out.SetPrefix(ds.prefix)

	// TODO: optimized 'for each set bit'
	for i := 0; i < ds.tableSize; i++ {
		if ds.bitfield.Bit(i) == 0 {
			continue
		}

		cindex := ds.indexForBitPos(i)
		ch := ds.children[cindex]
		if ch != nil {
			clnk, err := ch.Link()
			if err != nil {
				return nil, err
			}

			err = out.AddRawLink(ds.linkNamePrefix(i)+ch.Label(), clnk)
			if err != nil {
				return nil, err
			}
		} else {
			// child unloaded, just copy in link with updated name
			lnk := ds.nd.Links()[cindex]
			label := lnk.Name[ds.maxpadlen:]

			err := out.AddRawLink(ds.linkNamePrefix(i)+label, lnk)
			if err != nil {
				return nil, err
			}
		}
	}

	typ := upb.Data_HAMTShard
	data, err := proto.Marshal(&upb.Data{
		Type:     &typ,
		Fanout:   proto.Uint64(uint64(ds.tableSize)),
		HashType: proto.Uint64(HashMurmur3),
		Data:     ds.bitfield.Bytes(),
	})
	if err != nil {
		return nil, err
	}

	out.SetData(data)

	_, err = ds.dserv.Add(out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type shardValue struct {
	key string
	val *node.Link
}

// Link returns a link to this node
func (sv *shardValue) Link() (*node.Link, error) {
	return sv.val, nil
}

func (sv *shardValue) Label() string {
	return sv.key
}

func hash(val []byte) []byte {
	h := murmur3.New64()
	h.Write(val)
	return h.Sum(nil)
}

// Label for HamtShards is the empty string, this is used to differentiate them from
// value entries
func (ds *HamtShard) Label() string {
	return ""
}

// Set sets 'name' = nd in the HAMT
func (ds *HamtShard) Set(ctx context.Context, name string, nd node.Node) error {
	hv := &hashBits{b: hash([]byte(name))}
	_, err := ds.dserv.Add(nd)
	if err != nil {
		return err
	}

	lnk, err := node.MakeLink(nd)
	if err != nil {
		return err
	}
	lnk.Name = ds.linkNamePrefix(0) + name

	return ds.modifyValue(ctx, hv, name, lnk)
}

// Remove deletes the named entry if it exists, this operation is idempotent.
func (ds *HamtShard) Remove(ctx context.Context, name string) error {
	hv := &hashBits{b: hash([]byte(name))}
	return ds.modifyValue(ctx, hv, name, nil)
}

// Find searches for a child node by 'name' within this hamt
func (ds *HamtShard) Find(ctx context.Context, name string) (*node.Link, error) {
	hv := &hashBits{b: hash([]byte(name))}

	var out *node.Link
	err := ds.getValue(ctx, hv, name, func(sv *shardValue) error {
		out = sv.val
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// getChild returns the i'th child of this shard. If it is cached in the
// children array, it will return it from there. Otherwise, it loads the child
// node from disk.
func (ds *HamtShard) getChild(ctx context.Context, i int) (child, error) {
	if i >= len(ds.children) || i < 0 {
		return nil, fmt.Errorf("invalid index passed to getChild (likely corrupt bitfield)")
	}

	if len(ds.children) != len(ds.nd.Links()) {
		return nil, fmt.Errorf("inconsistent lengths between children array and Links array")
	}

	c := ds.children[i]
	if c != nil {
		return c, nil
	}

	return ds.loadChild(ctx, i)
}

// loadChild reads the i'th child node of this shard from disk and returns it
// as a 'child' interface
func (ds *HamtShard) loadChild(ctx context.Context, i int) (child, error) {
	lnk := ds.nd.Links()[i]
	if len(lnk.Name) < ds.maxpadlen {
		return nil, fmt.Errorf("invalid link name '%s'", lnk.Name)
	}

	nd, err := lnk.GetNode(ctx, ds.dserv)
	if err != nil {
		return nil, err
	}

	var c child
	if len(lnk.Name) == ds.maxpadlen {
		pbnd, ok := nd.(*dag.ProtoNode)
		if !ok {
			return nil, dag.ErrNotProtobuf
		}

		pbd, err := format.FromBytes(pbnd.Data())
		if err != nil {
			return nil, err
		}

		if pbd.GetType() != format.THAMTShard {
			return nil, fmt.Errorf("HAMT entries must have non-zero length name")
		}

		cds, err := NewHamtFromDag(ds.dserv, nd)
		if err != nil {
			return nil, err
		}

		c = cds
	} else {
		lnk2 := *lnk
		c = &shardValue{
			key: lnk.Name[ds.maxpadlen:],
			val: &lnk2,
		}
	}

	ds.children[i] = c
	return c, nil
}

func (ds *HamtShard) setChild(i int, c child) {
	ds.children[i] = c
}

// Link returns a merklelink to this shard node
func (ds *HamtShard) Link() (*node.Link, error) {
	nd, err := ds.Node()
	if err != nil {
		return nil, err
	}

	_, err = ds.dserv.Add(nd)
	if err != nil {
		return nil, err
	}

	return node.MakeLink(nd)
}

func (ds *HamtShard) insertChild(idx int, key string, lnk *node.Link) error {
	if lnk == nil {
		return os.ErrNotExist
	}

	i := ds.indexForBitPos(idx)
	ds.bitfield.SetBit(ds.bitfield, idx, 1)

	lnk.Name = ds.linkNamePrefix(idx) + key
	sv := &shardValue{
		key: key,
		val: lnk,
	}

	ds.children = append(ds.children[:i], append([]child{sv}, ds.children[i:]...)...)
	ds.nd.SetLinks(append(ds.nd.Links()[:i], append([]*node.Link{nil}, ds.nd.Links()[i:]...)...))
	return nil
}

func (ds *HamtShard) rmChild(i int) error {
	if i < 0 || i >= len(ds.children) || i >= len(ds.nd.Links()) {
		return fmt.Errorf("hamt: attempted to remove child with out of range index")
	}

	copy(ds.children[i:], ds.children[i+1:])
	ds.children = ds.children[:len(ds.children)-1]

	copy(ds.nd.Links()[i:], ds.nd.Links()[i+1:])
	ds.nd.SetLinks(ds.nd.Links()[:len(ds.nd.Links())-1])

	return nil
}

func (ds *HamtShard) getValue(ctx context.Context, hv *hashBits, key string, cb func(*shardValue) error) error {
	idx := hv.Next(ds.tableSizeLg2)
	if ds.bitfield.Bit(int(idx)) == 1 {
		cindex := ds.indexForBitPos(idx)

		child, err := ds.getChild(ctx, cindex)
		if err != nil {
			return err
		}

		switch child := child.(type) {
		case *HamtShard:
			return child.getValue(ctx, hv, key, cb)
		case *shardValue:
			if child.key == key {
				return cb(child)
			}
		}
	}

	return os.ErrNotExist
}

func (ds *HamtShard) EnumLinks(ctx context.Context) ([]*node.Link, error) {
	var links []*node.Link
	err := ds.ForEachLink(ctx, func(l *node.Link) error {
		links = append(links, l)
		return nil
	})
	return links, err
}

func (ds *HamtShard) ForEachLink(ctx context.Context, f func(*node.Link) error) error {
	return ds.walkTrie(ctx, func(sv *shardValue) error {
		lnk := sv.val
		lnk.Name = sv.key

		return f(lnk)
	})
}

func (ds *HamtShard) walkTrie(ctx context.Context, cb func(*shardValue) error) error {
	for i := 0; i < ds.tableSize; i++ {
		if ds.bitfield.Bit(i) == 0 {
			continue
		}

		idx := ds.indexForBitPos(i)
		// NOTE: an optimized version could simply iterate over each
		//       element in the 'children' array.
		c, err := ds.getChild(ctx, idx)
		if err != nil {
			return err
		}

		switch c := c.(type) {
		case *shardValue:
			err := cb(c)
			if err != nil {
				return err
			}

		case *HamtShard:
			err := c.walkTrie(ctx, cb)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected child type: %#v", c)
		}
	}
	return nil
}

func (ds *HamtShard) modifyValue(ctx context.Context, hv *hashBits, key string, val *node.Link) error {
	idx := hv.Next(ds.tableSizeLg2)

	if ds.bitfield.Bit(idx) != 1 {
		return ds.insertChild(idx, key, val)
	}

	cindex := ds.indexForBitPos(idx)

	child, err := ds.getChild(ctx, cindex)
	if err != nil {
		return err
	}

	switch child := child.(type) {
	case *HamtShard:
		err := child.modifyValue(ctx, hv, key, val)
		if err != nil {
			return err
		}

		if val == nil {
			switch len(child.children) {
			case 0:
				// empty sub-shard, prune it
				// Note: this shouldnt normally ever happen
				//       in the event of another implementation creates flawed
				//       structures, this will help to normalize them.
				ds.bitfield.SetBit(ds.bitfield, idx, 0)
				return ds.rmChild(cindex)
			case 1:
				nchild, ok := child.children[0].(*shardValue)
				if ok {
					// sub-shard with a single value element, collapse it
					ds.setChild(cindex, nchild)
				}
				return nil
			}
		}

		return nil
	case *shardValue:
		switch {
		case val == nil: // passing a nil value signifies a 'delete'
			ds.bitfield.SetBit(ds.bitfield, idx, 0)
			return ds.rmChild(cindex)

		case child.key == key: // value modification
			child.val = val
			return nil

		default: // replace value with another shard, one level deeper
			ns, err := NewHamtShard(ds.dserv, ds.tableSize)
			if err != nil {
				return err
			}
			ns.prefix = ds.prefix
			chhv := &hashBits{
				b:        hash([]byte(child.key)),
				consumed: hv.consumed,
			}

			err = ns.modifyValue(ctx, hv, key, val)
			if err != nil {
				return err
			}

			err = ns.modifyValue(ctx, chhv, child.key, child.val)
			if err != nil {
				return err
			}

			ds.setChild(cindex, ns)
			return nil
		}
	default:
		return fmt.Errorf("unexpected type for child: %#v", child)
	}
}

// indexForBitPos returns the index within the collapsed array corresponding to
// the given bit in the bitset.  The collapsed array contains only one entry
// per bit set in the bitfield, and this function is used to map the indices.
func (ds *HamtShard) indexForBitPos(bp int) int {
	// TODO: an optimization could reuse the same 'mask' here and change the size
	//       as needed. This isnt yet done as the bitset package doesnt make it easy
	//       to do.

	// make a bitmask (all bits set) 'bp' bits long
	mask := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(bp)), nil), big.NewInt(1))
	mask.And(mask, ds.bitfield)

	return popCount(mask)
}

// linkNamePrefix takes in the bitfield index of an entry and returns its hex prefix
func (ds *HamtShard) linkNamePrefix(idx int) string {
	return fmt.Sprintf(ds.prefixPadStr, idx)
}
