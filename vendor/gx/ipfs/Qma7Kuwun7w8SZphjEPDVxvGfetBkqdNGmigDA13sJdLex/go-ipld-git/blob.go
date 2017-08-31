package ipldgit

import (
	"errors"

	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

type Blob []byte

func (b Blob) Cid() *cid.Cid {
	c, _ := cid.Prefix{
		MhType:   mh.SHA1,
		MhLength: -1,
		Codec:    cid.GitRaw,
		Version:  1,
	}.Sum([]byte(b))
	return c
}

func (b Blob) Copy() node.Node {
	out := make([]byte, len(b))
	copy(out, b)
	return Blob(out)
}

func (b Blob) Links() []*node.Link {
	return nil
}

func (b Blob) Resolve(_ []string) (interface{}, []string, error) {
	return nil, nil, errors.New("no such link")
}

func (b Blob) ResolveLink(_ []string) (*node.Link, []string, error) {
	return nil, nil, errors.New("no such link")
}

func (b Blob) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "git_blob",
	}
}

func (b Blob) RawData() []byte {
	return []byte(b)
}

func (b Blob) Size() (uint64, error) {
	return uint64(len(b)), nil
}

func (b Blob) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

func (b Blob) String() string {
	return "[git blob]"
}

func (b Blob) Tree(p string, depth int) []string {
	return nil
}

func (b Blob) GitSha() []byte {
	return cidToSha(b.Cid())
}

var _ node.Node = (Blob)(nil)
