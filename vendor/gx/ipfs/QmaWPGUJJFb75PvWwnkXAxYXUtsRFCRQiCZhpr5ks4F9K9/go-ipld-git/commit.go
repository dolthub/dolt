package ipldgit

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

type Commit struct {
	DataSize  string      `json:"-"`
	GitTree   *cid.Cid    `json:"tree"`
	Parents   []*cid.Cid  `json:"parents"`
	Message   string      `json:"message"`
	Author    *PersonInfo `json:"author"`
	Committer *PersonInfo `json:"committer"`
	Encoding  string      `json:"encoding,omitempty"`
	Sig       *GpgSig     `json:"signature,omitempty"`
	MergeTag  []*MergeTag `json:"mergetag,omitempty"`

	// Other contains all the non-standard headers, such as 'HG:extra'
	Other []string `json:"other,omitempty"`

	cid *cid.Cid
}

type PersonInfo struct {
	Name     string
	Email    string
	Date     string
	Timezone string
}

func (pi *PersonInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"name":  pi.Name,
		"email": pi.Email,
		"date":  pi.Date + " " + pi.Timezone,
	})
}

func (pi *PersonInfo) String() string {
	f := "%s <%s>"
	arg := []interface{}{pi.Name, pi.Email}
	if pi.Date != "" {
		f = f + " %s"
		arg = append(arg, pi.Date)
	}

	if pi.Timezone != "" {
		f = f + " %s"
		arg = append(arg, pi.Timezone)
	}
	return fmt.Sprintf(f, arg...)
}

func (pi *PersonInfo) tree(name string, depth int) []string {
	if depth == 1 {
		return []string{name}
	}
	return []string{name + "/name", name + "/email", name + "/date"}
}

func (pi *PersonInfo) resolve(p []string) (interface{}, []string, error) {
	switch p[0] {
	case "name":
		return pi.Name, p[1:], nil
	case "email":
		return pi.Email, p[1:], nil
	case "date":
		return pi.Date + " " + pi.Timezone, p[1:], nil
	default:
		return nil, nil, errors.New("no such link")
	}
}

type MergeTag struct {
	Object *cid.Cid
	Type   string
	Tag    string
	Tagger *PersonInfo
	Text   string
}

type GpgSig struct {
	Text string
}

func (c *Commit) Cid() *cid.Cid {
	return c.cid
}

func (c *Commit) Copy() node.Node {
	nc := *c
	return &nc
}

func (c *Commit) Links() []*node.Link {
	out := []*node.Link{
		{Cid: c.GitTree},
	}

	for _, p := range c.Parents {
		out = append(out, &node.Link{Cid: p})
	}
	return out
}

func (c *Commit) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"type": "git_commit",
	}
}

func (c *Commit) RawData() []byte {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf, "commit %s\x00", c.DataSize)
	fmt.Fprintf(buf, "tree %s\n", hex.EncodeToString(cidToSha(c.GitTree)))
	for _, p := range c.Parents {
		fmt.Fprintf(buf, "parent %s\n", hex.EncodeToString(cidToSha(p)))
	}
	fmt.Fprintf(buf, "author %s\n", c.Author.String())
	fmt.Fprintf(buf, "committer %s\n", c.Committer.String())
	if len(c.Encoding) > 0 {
		fmt.Fprintf(buf, "encoding %s\n", c.Encoding)
	}
	for _, mtag := range c.MergeTag {
		fmt.Fprintf(buf, "mergetag object %s\n", hex.EncodeToString(cidToSha(mtag.Object)))
		fmt.Fprintf(buf, " type %s\n", mtag.Type)
		fmt.Fprintf(buf, " tag %s\n", mtag.Tag)
		fmt.Fprintf(buf, " tagger %s\n \n", mtag.Tagger.String())
		fmt.Fprintf(buf, "%s", mtag.Text)
	}
	if c.Sig != nil {
		fmt.Fprintln(buf, "gpgsig -----BEGIN PGP SIGNATURE-----")
		fmt.Fprint(buf, c.Sig.Text)
		fmt.Fprintln(buf, " -----END PGP SIGNATURE-----")
	}
	for _, line := range c.Other {
		fmt.Fprintln(buf, line)
	}
	fmt.Fprintf(buf, "\n%s", c.Message)
	return buf.Bytes()
}

func (c *Commit) Resolve(path []string) (interface{}, []string, error) {
	if len(path) == 0 {
		return nil, nil, fmt.Errorf("zero length path")
	}

	switch path[0] {
	case "parents":
		if len(path) == 1 {
			return c.Parents, nil, nil
		}

		i, err := strconv.Atoi(path[1])
		if err != nil {
			return nil, nil, err
		}

		if i < 0 || i >= len(c.Parents) {
			return nil, nil, fmt.Errorf("index out of range")
		}

		return &node.Link{Cid: c.Parents[i]}, path[2:], nil
	case "author":
		if len(path) == 1 {
			return c.Author, nil, nil
		}
		return c.Author.resolve(path[1:])
	case "committer":
		if len(path) == 1 {
			return c.Committer, nil, nil
		}
		return c.Committer.resolve(path[1:])
	case "signature":
		return c.Sig.Text, path[1:], nil
	case "message":
		return c.Message, path[1:], nil
	case "tree":
		return &node.Link{Cid: c.GitTree}, path[1:], nil
	default:
		return nil, nil, errors.New("no such link")
	}
}

func (c *Commit) ResolveLink(path []string) (*node.Link, []string, error) {
	out, rest, err := c.Resolve(path)
	if err != nil {
		return nil, nil, err
	}

	lnk, ok := out.(*node.Link)
	if !ok {
		return nil, nil, errors.New("not a link")
	}

	return lnk, rest, nil
}

func (c *Commit) Size() (uint64, error) {
	return 42, nil // close enough
}

func (c *Commit) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{}, nil
}

func (c *Commit) String() string {
	return "[git commit object]"
}

func (c *Commit) Tree(p string, depth int) []string {
	if depth != -1 {
		panic("proper tree not yet implemented")
	}
	tree := []string{"tree", "parents", "message", "gpgsig"}
	tree = append(tree, c.Author.tree("author", depth)...)
	tree = append(tree, c.Committer.tree("committer", depth)...)
	for i := range c.Parents {
		tree = append(tree, fmt.Sprintf("parents/%d", i))
	}
	return tree
}

func (c *Commit) GitSha() []byte {
	return cidToSha(c.Cid())
}

var _ node.Node = (*Commit)(nil)
