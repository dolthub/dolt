package coredag

import (
	"io"
	"io/ioutil"
	"math"

	"github.com/ipfs/go-ipfs/merkledag"

	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

func dagpbJSONParser(r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	nd := &merkledag.ProtoNode{}

	err = nd.UnmarshalJSON(data)
	if err != nil {
		return nil, err
	}

	nd.SetPrefix(cidPrefix(mhType, mhLen))

	return []node.Node{nd}, nil
}

func dagpbRawParser(r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	nd, err := merkledag.DecodeProtobuf(data)
	if err != nil {
		return nil, err
	}

	nd.SetPrefix(cidPrefix(mhType, mhLen))

	return []node.Node{nd}, nil
}

func cidPrefix(mhType uint64, mhLen int) *cid.Prefix {
	if mhType == math.MaxUint64 {
		mhType = mh.SHA2_256
	}

	prefix := &cid.Prefix{
		MhType:   mhType,
		MhLength: mhLen,
		Version:  1,
		Codec:    cid.DagProtobuf,
	}

	if mhType == mh.SHA2_256 {
		prefix.Version = 0
	}

	return prefix
}
