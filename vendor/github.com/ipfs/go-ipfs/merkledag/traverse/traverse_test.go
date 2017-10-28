package traverse

import (
	"bytes"
	"fmt"
	"testing"

	mdag "github.com/ipfs/go-ipfs/merkledag"
	mdagtest "github.com/ipfs/go-ipfs/merkledag/test"

	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

func TestDFSPreNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPre, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aab
1 /a/ab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestDFSPreSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPre, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aab
1 /a/ab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestDFSPostNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPost, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
0 /a
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
2 /a/aa/aaa
2 /a/aa/aab
1 /a/aa
2 /a/ab/aba
2 /a/ab/abb
1 /a/ab
0 /a
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))
}

func TestDFSPostSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPost, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
0 /a
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
2 /a/aa/aaa
2 /a/aa/aab
1 /a/aa
2 /a/ab/aba
2 /a/ab/abb
1 /a/ab
0 /a
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))
}

func TestBFSNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: BFS, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
2 /a/aa/aaa
2 /a/aa/aab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aaa
2 /a/aa/aaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestBFSSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: BFS, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
2 /a/aa/aaa
2 /a/aa/aab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func testWalkOutputs(t *testing.T, root node.Node, opts Options, expect []byte) {
	expect = bytes.TrimLeft(expect, "\n")

	buf := new(bytes.Buffer)
	walk := func(current State) error {
		s := fmt.Sprintf("%d %s\n", current.Depth, current.Node.(*mdag.ProtoNode).Data())
		t.Logf("walk: %s", s)
		buf.Write([]byte(s))
		return nil
	}

	opts.Func = walk
	if err := Traverse(root, opts); err != nil {
		t.Error(err)
		return
	}

	actual := buf.Bytes()
	if !bytes.Equal(actual, expect) {
		t.Error("error: outputs differ")
		t.Logf("expect:\n%s", expect)
		t.Logf("actual:\n%s", actual)
	} else {
		t.Logf("expect matches actual:\n%s", expect)
	}
}

func newFan(t *testing.T, ds mdag.DAGService) node.Node {
	a := mdag.NodeWithData([]byte("/a"))
	addLink(t, ds, a, child(t, ds, a, "aa"))
	addLink(t, ds, a, child(t, ds, a, "ab"))
	addLink(t, ds, a, child(t, ds, a, "ac"))
	addLink(t, ds, a, child(t, ds, a, "ad"))
	return a
}

func newLinkedList(t *testing.T, ds mdag.DAGService) node.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	aaa := child(t, ds, aa, "aaa")
	aaaa := child(t, ds, aaa, "aaaa")
	aaaaa := child(t, ds, aaaa, "aaaaa")
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, a, aa)
	return a
}

func newBinaryTree(t *testing.T, ds mdag.DAGService) node.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	ab := child(t, ds, a, "ab")
	addLink(t, ds, aa, child(t, ds, aa, "aaa"))
	addLink(t, ds, aa, child(t, ds, aa, "aab"))
	addLink(t, ds, ab, child(t, ds, ab, "aba"))
	addLink(t, ds, ab, child(t, ds, ab, "abb"))
	addLink(t, ds, a, aa)
	addLink(t, ds, a, ab)
	return a
}

func newBinaryDAG(t *testing.T, ds mdag.DAGService) node.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	aaa := child(t, ds, aa, "aaa")
	aaaa := child(t, ds, aaa, "aaaa")
	aaaaa := child(t, ds, aaaa, "aaaaa")
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, a, aa)
	addLink(t, ds, a, aa)
	return a
}

func addLink(t *testing.T, ds mdag.DAGService, a, b node.Node) {
	to := string(a.(*mdag.ProtoNode).Data()) + "2" + string(b.(*mdag.ProtoNode).Data())
	if _, err := ds.Add(b); err != nil {
		t.Error(err)
	}
	if err := a.(*mdag.ProtoNode).AddNodeLink(to, b.(*mdag.ProtoNode)); err != nil {
		t.Error(err)
	}
}

func child(t *testing.T, ds mdag.DAGService, a node.Node, name string) node.Node {
	return mdag.NodeWithData([]byte(string(a.(*mdag.ProtoNode).Data()) + "/" + name))
}
