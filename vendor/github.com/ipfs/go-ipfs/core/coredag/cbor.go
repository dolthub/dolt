package coredag

import (
	"io"
	"io/ioutil"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	ipldcbor "gx/ipfs/QmWCs8kMecJwCPK8JThue8TjgM2ieJ2HjTLDu7Cv2NEmZi/go-ipld-cbor"
)

func cborJSONParser(r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	nd, err := ipldcbor.FromJson(r, mhType, mhLen)
	if err != nil {
		return nil, err
	}

	return []node.Node{nd}, nil
}

func cborRawParser(r io.Reader, mhType uint64, mhLen int) ([]node.Node, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	nd, err := ipldcbor.Decode(data, mhType, mhLen)
	if err != nil {
		return nil, err
	}

	return []node.Node{nd}, nil
}
