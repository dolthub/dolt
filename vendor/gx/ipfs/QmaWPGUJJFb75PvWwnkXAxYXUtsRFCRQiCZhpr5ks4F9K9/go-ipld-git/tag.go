package ipldgit

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"errors"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type Tag struct {
	Object   *cid.Cid    `json:"object"`
	Type     string      `json:"type"`
	Tag      string      `json:"tag"`
	Tagger   *PersonInfo `json:"tagger"`
	Message  string      `json:"message"`
	dataSize string

	cid *cid.Cid
}

func (t *Tag) Cid() *cid.Cid {
	return t.cid
}

func (t *Tag) Copy() node.Node {
	nt := *t
	return &nt
}

func (t *Tag) Links() []*node.Link {
	return []*node.Link{{Cid: t.Object}}
}

func (t *Tag) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "git_tag",
	}
}

func (t *Tag) RawData() []byte {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "tag %s\x00", t.dataSize)
	fmt.Fprintf(buf, "object %s\n", hex.EncodeToString(cidToSha(t.Object)))
	fmt.Fprintf(buf, "type %s\n", t.Type)
	fmt.Fprintf(buf, "tag %s\n", t.Tag)
	if t.Tagger != nil {
		fmt.Fprintf(buf, "tagger %s\n", t.Tagger.String())
	}
	if t.Message != "" {
		fmt.Fprintf(buf, "\n%s", t.Message)
	}
	return buf.Bytes()
}

func (t *Tag) Resolve(path []string) (interface{}, []string, error) {
	if len(path) == 0 {
		return nil, nil, fmt.Errorf("zero length path")
	}

	switch path[0] {
	case "object":
		return &node.Link{Cid: t.Object}, path[1:], nil
	case "type":
		return t.Type, path[1:], nil
	case "tagger":
		if len(path) == 1 {
			return t.Tagger, nil, nil
		}
		return t.Tagger.resolve(path[1:])
	case "message":
		return t.Message, path[1:], nil
	case "tag":
		return t.Tag, path[1:], nil
	default:
		return nil, nil, errors.New("no such link")
	}
}

func (t *Tag) ResolveLink(path []string) (*node.Link, []string, error) {
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

func (t *Tag) Size() (uint64, error) {
	return 42, nil // close enough
}

func (t *Tag) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

func (t *Tag) String() string {
	return "[git tag object]"
}

func (t *Tag) Tree(p string, depth int) []string {
	if p != "" {
		if p == "tagger" {
			return []string{"name", "email", "date"}
		}
		return nil
	}
	if depth == 0 {
		return nil
	}

	tree := []string{"object", "type", "tag", "message"}
	tree = append(tree, t.Tagger.tree("tagger", depth)...)
	return tree
}

func (t *Tag) GitSha() []byte {
	return cidToSha(t.Cid())
}

var _ node.Node = (*Tag)(nil)
