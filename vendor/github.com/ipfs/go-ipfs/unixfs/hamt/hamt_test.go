package hamt

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	dag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"
	dagutils "github.com/ipfs/go-ipfs/merkledag/utils"
	ft "github.com/ipfs/go-ipfs/unixfs"
)

func shuffle(seed int64, arr []string) {
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < len(arr); i++ {
		a := r.Intn(len(arr))
		b := r.Intn(len(arr))
		arr[a], arr[b] = arr[b], arr[a]
	}
}

func makeDir(ds dag.DAGService, size int) ([]string, *HamtShard, error) {
	return makeDirWidth(ds, size, 256)
}

func makeDirWidth(ds dag.DAGService, size, width int) ([]string, *HamtShard, error) {
	s, _ := NewHamtShard(ds, width)

	var dirs []string
	for i := 0; i < size; i++ {
		dirs = append(dirs, fmt.Sprintf("DIRNAME%d", i))
	}

	shuffle(time.Now().UnixNano(), dirs)

	for i := 0; i < len(dirs); i++ {
		nd := ft.EmptyDirNode()
		ds.Add(nd)
		err := s.Set(context.Background(), dirs[i], nd)
		if err != nil {
			return nil, nil, err
		}
	}

	return dirs, s, nil
}

func assertLink(s *HamtShard, name string, found bool) error {
	_, err := s.Find(context.Background(), name)
	switch err {
	case os.ErrNotExist:
		if found {
			return err
		}

		return nil
	case nil:
		if found {
			return nil
		}

		return fmt.Errorf("expected not to find link named %s", name)
	default:
		return err
	}
}

func assertSerializationWorks(ds dag.DAGService, s *HamtShard) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nd, err := s.Node()
	if err != nil {
		return err
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		return err
	}

	linksA, err := s.EnumLinks(ctx)
	if err != nil {
		return err
	}

	linksB, err := nds.EnumLinks(ctx)
	if err != nil {
		return err
	}

	if len(linksA) != len(linksB) {
		return fmt.Errorf("links arrays are different sizes")
	}

	for i, a := range linksA {
		b := linksB[i]
		if a.Name != b.Name {
			return fmt.Errorf("links names mismatch")
		}

		if a.Cid.String() != b.Cid.String() {
			return fmt.Errorf("link hashes dont match")
		}

		if a.Size != b.Size {
			return fmt.Errorf("link sizes not the same")
		}
	}

	return nil
}

func TestBasicSet(t *testing.T) {
	ds := mdtest.Mock()
	for _, w := range []int{128, 256, 512, 1024, 2048, 4096} {
		t.Run(fmt.Sprintf("BasicSet%d", w), func(t *testing.T) {
			names, s, err := makeDirWidth(ds, 1000, w)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			for _, d := range names {
				_, err := s.Find(ctx, d)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDirBuilding(t *testing.T) {
	ds := mdtest.Mock()
	_, _ = NewHamtShard(ds, 256)

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	//printDag(ds, nd, 0)

	k := nd.Cid()

	if k.String() != "QmY89TkSEVHykWMHDmyejSWFj9CYNtvzw4UwnT9xbc4Zjc" {
		t.Fatalf("output didnt match what we expected (got %s)", k.String())
	}
}

func TestShardReload(t *testing.T) {
	ds := mdtest.Mock()
	_, _ = NewHamtShard(ds, 256)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, s, err := makeDir(ds, 200)
	if err != nil {
		t.Fatal(err)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := nds.EnumLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 200 {
		t.Fatal("not enough links back")
	}

	_, err = nds.Find(ctx, "DIRNAME50")
	if err != nil {
		t.Fatal(err)
	}

	// Now test roundtrip marshal with no operations

	nds, err = NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	ond, err := nds.Node()
	if err != nil {
		t.Fatal(err)
	}

	outk := ond.Cid()
	ndk := nd.Cid()

	if !outk.Equals(ndk) {
		printDiff(ds, nd.(*dag.ProtoNode), ond.(*dag.ProtoNode))
		t.Fatal("roundtrip serialization failed")
	}
}

func TestRemoveElems(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 500)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	shuffle(time.Now().UnixNano(), dirs)

	for _, d := range dirs {
		err := s.Remove(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	if len(nd.Links()) > 0 {
		t.Fatal("shouldnt have any links here")
	}

	err = s.Remove(ctx, "doesnt exist")
	if err != os.ErrNotExist {
		t.Fatal("expected error does not exist")
	}
}

func TestSetAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 300)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	empty := ft.EmptyDirNode()
	for i := 0; i < 100; i++ {
		err := nds.Set(ctx, fmt.Sprintf("moredirs%d", i), empty)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 400 {
		t.Fatal("expected 400 links")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateAddShard(t *testing.T) {
	ds := mdtest.Mock()
	dir, _ := NewHamtShard(ds, 256)
	nd := new(dag.ProtoNode)
	ctx := context.Background()

	err := dir.Set(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	err = dir.Set(ctx, "test", nd)
	if err != nil {
		t.Fatal(err)
	}

	lnks, err := dir.EnumLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(lnks) != 1 {
		t.Fatal("expected only one link")
	}
}

func TestLoadFailsFromNonShard(t *testing.T) {
	ds := mdtest.Mock()
	nd := ft.EmptyDirNode()

	_, err := NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}

	nd = new(dag.ProtoNode)

	_, err = NewHamtFromDag(ds, nd)
	if err == nil {
		t.Fatal("expected dir shard creation to fail when given normal directory")
	}
}

func TestFindNonExisting(t *testing.T) {
	ds := mdtest.Mock()
	_, s, err := makeDir(ds, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 200; i++ {
		_, err := s.Find(ctx, fmt.Sprintf("notfound%d", i))
		if err != os.ErrNotExist {
			t.Fatal("expected ErrNotExist")
		}
	}
}

func TestRemoveElemsAfterMarshal(t *testing.T) {
	ds := mdtest.Mock()
	dirs, s, err := makeDir(ds, 30)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	sort.Strings(dirs)

	err = s.Remove(ctx, dirs[0])
	if err != nil {
		t.Fatal(err)
	}

	out, err := s.Find(ctx, dirs[0])
	if err == nil {
		t.Fatal("expected error, got: ", out)
	}

	nd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	nds, err := NewHamtFromDag(ds, nd)
	if err != nil {
		t.Fatal(err)
	}

	_, err = nds.Find(ctx, dirs[0])
	if err == nil {
		t.Fatal("expected not to find ", dirs[0])
	}

	for _, d := range dirs[1:] {
		_, err := nds.Find(ctx, d)
		if err != nil {
			t.Fatal("could not find expected link after unmarshaling")
		}
	}

	for _, d := range dirs[1:] {
		err := nds.Remove(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
	}

	links, err := nds.EnumLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(links) != 0 {
		t.Fatal("expected all links to be removed")
	}

	err = assertSerializationWorks(ds, nds)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBitfieldIndexing(t *testing.T) {
	ds := mdtest.Mock()
	s, _ := NewHamtShard(ds, 256)

	set := func(i int) {
		s.bitfield.SetBit(s.bitfield, i, 1)
	}

	assert := func(i int, val int) {
		if s.indexForBitPos(i) != val {
			t.Fatalf("expected index %d to be %d", i, val)
		}
	}

	assert(50, 0)
	set(4)
	set(5)
	set(60)

	assert(10, 2)
	set(3)
	assert(10, 3)
	assert(1, 0)

	assert(100, 4)
	set(50)
	assert(45, 3)
	set(100)
	assert(100, 5)
}

// test adding a sharded directory node as the child of another directory node.
// if improperly implemented, the parent hamt may assume the child is a part of
// itself.
func TestSetHamtChild(t *testing.T) {
	ds := mdtest.Mock()
	s, _ := NewHamtShard(ds, 256)
	ctx := context.Background()

	e := ft.EmptyDirNode()
	ds.Add(e)

	err := s.Set(ctx, "bar", e)
	if err != nil {
		t.Fatal(err)
	}

	snd, err := s.Node()
	if err != nil {
		t.Fatal(err)
	}

	_, ns, err := makeDir(ds, 50)
	if err != nil {
		t.Fatal(err)
	}

	err = ns.Set(ctx, "foo", snd)
	if err != nil {
		t.Fatal(err)
	}

	nsnd, err := ns.Node()
	if err != nil {
		t.Fatal(err)
	}

	hs, err := NewHamtFromDag(ds, nsnd)
	if err != nil {
		t.Fatal(err)
	}

	err = assertLink(hs, "bar", false)
	if err != nil {
		t.Fatal(err)
	}

	err = assertLink(hs, "foo", true)
	if err != nil {
		t.Fatal(err)
	}
}

func printDiff(ds dag.DAGService, a, b *dag.ProtoNode) {
	diff, err := dagutils.Diff(context.TODO(), ds, a, b)
	if err != nil {
		panic(err)
	}

	for _, d := range diff {
		fmt.Println(d)
	}
}

func BenchmarkHAMTSet(b *testing.B) {
	ds := mdtest.Mock()
	sh, _ := NewHamtShard(ds, 256)
	nd, err := sh.Node()
	if err != nil {
		b.Fatal(err)
	}

	_, err = ds.Add(nd)
	if err != nil {
		b.Fatal(err)
	}
	ds.Add(ft.EmptyDirNode())

	for i := 0; i < b.N; i++ {
		s, err := NewHamtFromDag(ds, nd)
		if err != nil {
			b.Fatal(err)
		}

		err = s.Set(context.TODO(), fmt.Sprint(i), ft.EmptyDirNode())
		if err != nil {
			b.Fatal(err)
		}

		out, err := s.Node()
		if err != nil {
			b.Fatal(err)
		}

		nd = out
	}
}

func TestHamtBadSize(t *testing.T) {
	_, err := NewHamtShard(nil, 7)
	if err == nil {
		t.Fatal("should have failed to construct hamt with bad size")
	}
}
