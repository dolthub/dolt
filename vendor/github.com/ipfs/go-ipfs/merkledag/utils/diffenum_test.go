package dagutils

import (
	"context"
	"fmt"
	"testing"

	dag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

func buildNode(name string, desc map[string]ndesc, out map[string]node.Node) node.Node {
	this := desc[name]
	nd := new(dag.ProtoNode)
	nd.SetData([]byte(name))
	for k, v := range this {
		child, ok := out[v]
		if !ok {
			child = buildNode(v, desc, out)
			out[v] = child
		}

		if err := nd.AddNodeLink(k, child); err != nil {
			panic(err)
		}
	}

	return nd
}

type ndesc map[string]string

func mkGraph(desc map[string]ndesc) map[string]node.Node {
	out := make(map[string]node.Node)
	for name := range desc {
		if _, ok := out[name]; ok {
			continue
		}

		out[name] = buildNode(name, desc, out)
	}
	return out
}

var tg1 = map[string]ndesc{
	"a1": ndesc{
		"foo": "b",
	},
	"b": ndesc{},
	"a2": ndesc{
		"foo": "b",
		"bar": "c",
	},
	"c": ndesc{},
}

var tg2 = map[string]ndesc{
	"a1": ndesc{
		"foo": "b",
	},
	"b": ndesc{},
	"a2": ndesc{
		"foo": "b",
		"bar": "c",
	},
	"c": ndesc{"baz": "d"},
	"d": ndesc{},
}

var tg3 = map[string]ndesc{
	"a1": ndesc{
		"foo": "b",
		"bar": "c",
	},
	"b": ndesc{},
	"a2": ndesc{
		"foo": "b",
		"bar": "d",
	},
	"c": ndesc{},
	"d": ndesc{},
}

func TestDiffEnumBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg1)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, nd := range nds {
		_, err := ds.Add(nd)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if err != nil {
		t.Fatal(err)
	}

	err = assertCidList(lgds.log, []*cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid()})
	if err != nil {
		t.Fatal(err)
	}
}

type getLogger struct {
	ds  node.NodeGetter
	log []*cid.Cid
}

func (gl *getLogger) Get(ctx context.Context, c *cid.Cid) (node.Node, error) {
	nd, err := gl.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	gl.log = append(gl.log, c)
	return nd, nil
}

func assertCidList(a, b []*cid.Cid) error {
	if len(a) != len(b) {
		return fmt.Errorf("got different number of cids than expected")
	}
	for i, c := range a {
		if !c.Equals(b[i]) {
			return fmt.Errorf("expected %s, got %s", c, b[i])
		}
	}
	return nil
}

func TestDiffEnumFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg2)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, s := range []string{"a1", "a2", "b", "c"} {
		_, err := ds.Add(nds[s])
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if err != dag.ErrNotFound {
		t.Fatal("expected err not found")
	}

	err = assertCidList(lgds.log, []*cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid()})
	if err != nil {
		t.Fatal(err)
	}

}

func TestDiffEnumRecurse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg3)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, s := range []string{"a1", "a2", "b", "c", "d"} {
		_, err := ds.Add(nds[s])
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if err != nil {
		t.Fatal(err)
	}

	err = assertCidList(lgds.log, []*cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid(), nds["d"].Cid()})
	if err != nil {
		t.Fatal(err)
	}
}
