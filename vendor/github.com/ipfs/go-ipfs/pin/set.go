package pin

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/ipfs/go-ipfs/merkledag"
	"github.com/ipfs/go-ipfs/pin/internal/pb"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	"gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
)

const (
	// defaultFanout specifies the default number of fan-out links per layer
	defaultFanout = 256

	// maxItems is the maximum number of items that will fit in a single bucket
	maxItems = 8192
)

func hash(seed uint32, c *cid.Cid) uint32 {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], seed)
	h := fnv.New32a()
	_, _ = h.Write(buf[:])
	_, _ = h.Write(c.Bytes())
	return h.Sum32()
}

type itemIterator func() (c *cid.Cid, ok bool)

type keyObserver func(*cid.Cid)

type sortByHash struct {
	links []*node.Link
}

func (s sortByHash) Len() int {
	return len(s.links)
}

func (s sortByHash) Less(a, b int) bool {
	return bytes.Compare(s.links[a].Cid.Bytes(), s.links[b].Cid.Bytes()) == -1
}

func (s sortByHash) Swap(a, b int) {
	s.links[a], s.links[b] = s.links[b], s.links[a]
}

func storeItems(ctx context.Context, dag merkledag.DAGService, estimatedLen uint64, depth uint32, iter itemIterator, internalKeys keyObserver) (*merkledag.ProtoNode, error) {
	links := make([]*node.Link, 0, defaultFanout+maxItems)
	for i := 0; i < defaultFanout; i++ {
		links = append(links, &node.Link{Cid: emptyKey})
	}

	// add emptyKey to our set of internal pinset objects
	n := &merkledag.ProtoNode{}
	n.SetLinks(links)

	internalKeys(emptyKey)

	hdr := &pb.Set{
		Version: proto.Uint32(1),
		Fanout:  proto.Uint32(defaultFanout),
		Seed:    proto.Uint32(depth),
	}
	if err := writeHdr(n, hdr); err != nil {
		return nil, err
	}

	if estimatedLen < maxItems {
		// it'll probably fit
		links := n.Links()
		for i := 0; i < maxItems; i++ {
			k, ok := iter()
			if !ok {
				// all done
				break
			}

			links = append(links, &node.Link{Cid: k})
		}

		n.SetLinks(links)

		// sort by hash, also swap item Data
		s := sortByHash{
			links: n.Links()[defaultFanout:],
		}
		sort.Stable(s)
	}

	hashed := make([][]*cid.Cid, defaultFanout)
	for {
		// This loop essentially enumerates every single item in the set
		// and maps them all into a set of buckets. Each bucket will be recursively
		// turned into its own sub-set, and so on down the chain. Each sub-set
		// gets added to the dagservice, and put into its place in a set nodes
		// links array.
		//
		// Previously, the bucket was selected by taking an int32 from the hash of
		// the input key + seed. This was erroneous as we would later be assigning
		// the created sub-sets into an array of length 256 by the modulus of the
		// int32 hash value with 256. This resulted in overwriting existing sub-sets
		// and losing pins. The fix (a few lines down from this comment), is to
		// map the hash value down to the 8 bit keyspace here while creating the
		// buckets. This way, we avoid any overlapping later on.
		k, ok := iter()
		if !ok {
			break
		}
		h := hash(depth, k) % defaultFanout
		hashed[h] = append(hashed[h], k)
	}

	for h, items := range hashed {
		if len(items) == 0 {
			// recursion base case
			continue
		}

		childIter := getCidListIterator(items)

		// recursively create a pinset from the items for this bucket index
		child, err := storeItems(ctx, dag, uint64(len(items)), depth+1, childIter, internalKeys)
		if err != nil {
			return nil, err
		}

		size, err := child.Size()
		if err != nil {
			return nil, err
		}

		childKey, err := dag.Add(child)
		if err != nil {
			return nil, err
		}

		internalKeys(childKey)

		// overwrite the 'empty key' in the existing links array
		n.Links()[h] = &node.Link{
			Cid:  childKey,
			Size: size,
		}
	}
	return n, nil
}

func readHdr(n *merkledag.ProtoNode) (*pb.Set, error) {
	hdrLenRaw, consumed := binary.Uvarint(n.Data())
	if consumed <= 0 {
		return nil, errors.New("invalid Set header length")
	}

	pbdata := n.Data()[consumed:]
	if hdrLenRaw > uint64(len(pbdata)) {
		return nil, errors.New("impossibly large Set header length")
	}
	// as hdrLenRaw was <= an int, we now know it fits in an int
	hdrLen := int(hdrLenRaw)
	var hdr pb.Set
	if err := proto.Unmarshal(pbdata[:hdrLen], &hdr); err != nil {
		return nil, err
	}

	if v := hdr.GetVersion(); v != 1 {
		return nil, fmt.Errorf("unsupported Set version: %d", v)
	}
	if uint64(hdr.GetFanout()) > uint64(len(n.Links())) {
		return nil, errors.New("impossibly large Fanout")
	}
	return &hdr, nil
}

func writeHdr(n *merkledag.ProtoNode, hdr *pb.Set) error {
	hdrData, err := proto.Marshal(hdr)
	if err != nil {
		return err
	}

	// make enough space for the length prefix and the marshalled header data
	data := make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64+len(hdrData))

	// write the uvarint length of the header data
	uvarlen := binary.PutUvarint(data, uint64(len(hdrData)))

	// append the actual protobuf data *after* the length value we wrote
	data = append(data[:uvarlen], hdrData...)

	n.SetData(data)
	return nil
}

type walkerFunc func(idx int, link *node.Link) error

func walkItems(ctx context.Context, dag merkledag.DAGService, n *merkledag.ProtoNode, fn walkerFunc, children keyObserver) error {
	hdr, err := readHdr(n)
	if err != nil {
		return err
	}
	// readHdr guarantees fanout is a safe value
	fanout := hdr.GetFanout()
	for i, l := range n.Links()[fanout:] {
		if err := fn(i, l); err != nil {
			return err
		}
	}
	for _, l := range n.Links()[:fanout] {
		c := l.Cid
		children(c)
		if c.Equals(emptyKey) {
			continue
		}
		subtree, err := l.GetNode(ctx, dag)
		if err != nil {
			return err
		}

		stpb, ok := subtree.(*merkledag.ProtoNode)
		if !ok {
			return merkledag.ErrNotProtobuf
		}

		if err := walkItems(ctx, dag, stpb, fn, children); err != nil {
			return err
		}
	}
	return nil
}

func loadSet(ctx context.Context, dag merkledag.DAGService, root *merkledag.ProtoNode, name string, internalKeys keyObserver) ([]*cid.Cid, error) {
	l, err := root.GetNodeLink(name)
	if err != nil {
		return nil, err
	}

	lnkc := l.Cid
	internalKeys(lnkc)

	n, err := l.GetNode(ctx, dag)
	if err != nil {
		return nil, err
	}

	pbn, ok := n.(*merkledag.ProtoNode)
	if !ok {
		return nil, merkledag.ErrNotProtobuf
	}

	var res []*cid.Cid
	walk := func(idx int, link *node.Link) error {
		res = append(res, link.Cid)
		return nil
	}

	if err := walkItems(ctx, dag, pbn, walk, internalKeys); err != nil {
		return nil, err
	}
	return res, nil
}

func getCidListIterator(cids []*cid.Cid) itemIterator {
	return func() (c *cid.Cid, ok bool) {
		if len(cids) == 0 {
			return nil, false
		}

		first := cids[0]
		cids = cids[1:]
		return first, true
	}
}

func storeSet(ctx context.Context, dag merkledag.DAGService, cids []*cid.Cid, internalKeys keyObserver) (*merkledag.ProtoNode, error) {
	iter := getCidListIterator(cids)

	n, err := storeItems(ctx, dag, uint64(len(cids)), 0, iter, internalKeys)
	if err != nil {
		return nil, err
	}
	c, err := dag.Add(n)
	if err != nil {
		return nil, err
	}
	internalKeys(c)
	return n, nil
}
