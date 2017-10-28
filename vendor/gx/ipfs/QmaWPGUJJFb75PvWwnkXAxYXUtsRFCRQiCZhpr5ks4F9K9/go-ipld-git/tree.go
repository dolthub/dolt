package ipldgit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"errors"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type Tree struct {
	entries map[string]*TreeEntry
	size    int
	order   []string
	cid     *cid.Cid
}

type TreeEntry struct {
	name string
	Mode string   `json:"mode"`
	Hash *cid.Cid `json:"hash"`
}

func (t *Tree) Cid() *cid.Cid {
	return t.cid
}

func (t *Tree) String() string {
	return "[git tree object]"
}

func (t *Tree) GitSha() []byte {
	return cidToSha(t.cid)
}

func (t *Tree) Copy() node.Node {
	out := &Tree{
		entries: make(map[string]*TreeEntry),
		cid:     t.cid,
		size:    t.size,
		order:   t.order, // TODO: make a deep copy of this
	}

	for k, v := range t.entries {
		nv := *v
		out.entries[k] = &nv
	}
	return out
}

func (t *Tree) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.entries)
}

func (t *Tree) Tree(p string, depth int) []string {
	if p != "" {
		_, ok := t.entries[p]
		if !ok {
			return nil
		}

		return []string{"mode", "type", "hash"}
	}

	if depth == 0 {
		return nil
	}

	if depth == 1 {
		return t.order
	}

	var out []string
	for k, _ := range t.entries {
		out = append(out, k, k+"/mode", k+"/type", k+"/hash")
	}
	return out
}

func (t *Tree) Links() []*node.Link {
	var out []*node.Link
	for _, v := range t.entries {
		out = append(out, &node.Link{Cid: v.Hash})
	}
	return out
}

func (t *Tree) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "git tree object",
	}
}

func (t *Tree) RawData() []byte {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "tree %d\x00", t.size)
	for _, s := range t.order {
		t.entries[s].WriteTo(buf)
	}
	return buf.Bytes()
}

func (t *Tree) Resolve(p []string) (interface{}, []string, error) {
	e, ok := t.entries[p[0]]
	if !ok {
		return nil, nil, errors.New("no such link")
	}

	if len(p) == 1 {
		return e, nil, nil
	}

	switch p[1] {
	case "hash":
		return &node.Link{Cid: e.Hash}, p[2:], nil
	case "mode":
		return e.Mode, p[2:], nil
	default:
		return nil, nil, errors.New("no such link")
	}
}

func (t Tree) ResolveLink(path []string) (*node.Link, []string, error) {
	out, rest, err := t.Resolve(path)
	if err != nil {
		return nil, nil, err
	}

	lnk, ok := out.(*node.Link)
	if !ok {
		return nil, nil, errors.New("not a link")
	}

	return lnk, rest, nil
}

func (t *Tree) Size() (uint64, error) {
	fmt.Println("size isnt implemented")
	return 13, nil // trees are probably smaller than commits, so 13 seems like a good number
}

func (t *Tree) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

func (te *TreeEntry) WriteTo(w io.Writer) (int, error) {
	n, err := fmt.Fprintf(w, "%s %s\x00", te.Mode, te.name)
	if err != nil {
		return 0, err
	}

	nn, err := w.Write(cidToSha(te.Hash))
	if err != nil {
		return n, err
	}

	return n + nn, nil
}

var _ node.Node = (*Tree)(nil)
