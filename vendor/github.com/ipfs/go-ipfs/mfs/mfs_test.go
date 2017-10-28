package mfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	bserv "github.com/ipfs/go-ipfs/blockservice"
	offline "github.com/ipfs/go-ipfs/exchange/offline"
	importer "github.com/ipfs/go-ipfs/importer"
	chunk "github.com/ipfs/go-ipfs/importer/chunk"
	dag "github.com/ipfs/go-ipfs/merkledag"
	"github.com/ipfs/go-ipfs/path"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dssync "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
)

func emptyDirNode() *dag.ProtoNode {
	return dag.NodeWithData(ft.FolderPBData())
}

func getDagserv(t *testing.T) dag.DAGService {
	db := dssync.MutexWrap(ds.NewMapDatastore())
	bs := bstore.NewBlockstore(db)
	blockserv := bserv.New(bs, offline.Exchange(bs))
	return dag.NewDAGService(blockserv)
}

func getRandFile(t *testing.T, ds dag.DAGService, size int64) node.Node {
	r := io.LimitReader(u.NewTimeSeededRand(), size)
	return fileNodeFromReader(t, ds, r)
}

func fileNodeFromReader(t *testing.T, ds dag.DAGService, r io.Reader) node.Node {
	nd, err := importer.BuildDagFromReader(ds, chunk.DefaultSplitter(r))
	if err != nil {
		t.Fatal(err)
	}
	return nd
}

func mkdirP(t *testing.T, root *Directory, pth string) *Directory {
	dirs := path.SplitList(pth)
	cur := root
	for _, d := range dirs {
		n, err := cur.Mkdir(d)
		if err != nil && err != os.ErrExist {
			t.Fatal(err)
		}
		if err == os.ErrExist {
			fsn, err := cur.Child(d)
			if err != nil {
				t.Fatal(err)
			}
			switch fsn := fsn.(type) {
			case *Directory:
				n = fsn
			case *File:
				t.Fatal("tried to make a directory where a file already exists")
			}
		}

		cur = n
	}
	return cur
}

func assertDirAtPath(root *Directory, pth string, children []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsn, err := DirLookup(root, pth)
	if err != nil {
		return err
	}

	dir, ok := fsn.(*Directory)
	if !ok {
		return fmt.Errorf("%s was not a directory", pth)
	}

	listing, err := dir.List(ctx)
	if err != nil {
		return err
	}

	var names []string
	for _, d := range listing {
		names = append(names, d.Name)
	}

	sort.Strings(children)
	sort.Strings(names)
	if !compStrArrs(children, names) {
		return errors.New("directories children did not match!")
	}

	return nil
}

func compStrArrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func assertFileAtPath(ds dag.DAGService, root *Directory, expn node.Node, pth string) error {
	exp, ok := expn.(*dag.ProtoNode)
	if !ok {
		return dag.ErrNotProtobuf
	}

	parts := path.SplitList(pth)
	cur := root
	for i, d := range parts[:len(parts)-1] {
		next, err := cur.Child(d)
		if err != nil {
			return fmt.Errorf("looking for %s failed: %s", pth, err)
		}

		nextDir, ok := next.(*Directory)
		if !ok {
			return fmt.Errorf("%s points to a non-directory", parts[:i+1])
		}

		cur = nextDir
	}

	last := parts[len(parts)-1]
	finaln, err := cur.Child(last)
	if err != nil {
		return err
	}

	file, ok := finaln.(*File)
	if !ok {
		return fmt.Errorf("%s was not a file!", pth)
	}

	rfd, err := file.Open(OpenReadOnly, false)
	if err != nil {
		return err
	}

	out, err := ioutil.ReadAll(rfd)
	if err != nil {
		return err
	}

	expbytes, err := catNode(ds, exp)
	if err != nil {
		return err
	}

	if !bytes.Equal(out, expbytes) {
		return fmt.Errorf("Incorrect data at path!")
	}
	return nil
}

func catNode(ds dag.DAGService, nd *dag.ProtoNode) ([]byte, error) {
	r, err := uio.NewDagReader(context.TODO(), nd, ds)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func setupRoot(ctx context.Context, t *testing.T) (dag.DAGService, *Root) {
	ds := getDagserv(t)

	root := emptyDirNode()
	rt, err := NewRoot(ctx, ds, root, func(ctx context.Context, c *cid.Cid) error {
		fmt.Println("PUBLISHED: ", c)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	return ds, rt
}

func TestBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	// test making a basic dir
	_, err := rootdir.Mkdir("a")
	if err != nil {
		t.Fatal(err)
	}

	path := "a/b/c/d/e/f/g"
	d := mkdirP(t, rootdir, path)

	fi := getRandFile(t, ds, 1000)

	// test inserting that file
	err = d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	err = assertFileAtPath(ds, rootdir, fi, "a/b/c/d/e/f/g/afile")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMkdir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	dirsToMake := []string{"a", "B", "foo", "bar", "cats", "fish"}
	sort.Strings(dirsToMake) // sort for easy comparing later

	for _, d := range dirsToMake {
		_, err := rootdir.Mkdir(d)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := assertDirAtPath(rootdir, "/", dirsToMake)
	if err != nil {
		t.Fatal(err)
	}

	for _, d := range dirsToMake {
		mkdirP(t, rootdir, "a/"+d)
	}

	err = assertDirAtPath(rootdir, "/a", dirsToMake)
	if err != nil {
		t.Fatal(err)
	}

	// mkdir over existing dir should fail
	_, err = rootdir.Mkdir("a")
	if err == nil {
		t.Fatal("should have failed!")
	}
}

func TestDirectoryLoadFromDag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	nd := getRandFile(t, ds, 1000)
	_, err := ds.Add(nd)
	if err != nil {
		t.Fatal(err)
	}

	fihash := nd.Cid()

	dir := emptyDirNode()
	_, err = ds.Add(dir)
	if err != nil {
		t.Fatal(err)
	}

	dirhash := dir.Cid()

	top := emptyDirNode()
	top.SetLinks([]*node.Link{
		{
			Name: "a",
			Cid:  fihash,
		},
		{
			Name: "b",
			Cid:  dirhash,
		},
	})

	err = rootdir.AddChild("foo", top)
	if err != nil {
		t.Fatal(err)
	}

	// get this dir
	topi, err := rootdir.Child("foo")
	if err != nil {
		t.Fatal(err)
	}

	topd := topi.(*Directory)

	// mkdir over existing but unloaded child file should fail
	_, err = topd.Mkdir("a")
	if err == nil {
		t.Fatal("expected to fail!")
	}

	// mkdir over existing but unloaded child dir should fail
	_, err = topd.Mkdir("b")
	if err == nil {
		t.Fatal("expected to fail!")
	}

	// adding a child over an existing path fails
	err = topd.AddChild("b", nd)
	if err == nil {
		t.Fatal("expected to fail!")
	}

	err = assertFileAtPath(ds, rootdir, nd, "foo/a")
	if err != nil {
		t.Fatal(err)
	}

	err = assertDirAtPath(rootdir, "foo/b", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = rootdir.Unlink("foo")
	if err != nil {
		t.Fatal(err)
	}

	err = assertDirAtPath(rootdir, "", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMfsFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	fisize := 1000
	nd := getRandFile(t, ds, 1000)

	err := rootdir.AddChild("file", nd)
	if err != nil {
		t.Fatal(err)
	}

	fsn, err := rootdir.Child("file")
	if err != nil {
		t.Fatal(err)
	}

	fi := fsn.(*File)

	if fi.Type() != TFile {
		t.Fatal("some is seriously wrong here")
	}

	wfd, err := fi.Open(OpenReadWrite, true)
	if err != nil {
		t.Fatal(err)
	}

	// assert size is as expected
	size, err := fi.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(fisize) {
		t.Fatal("size isnt correct")
	}

	// write to beginning of file
	b := []byte("THIS IS A TEST")
	n, err := wfd.Write(b)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(b) {
		t.Fatal("didnt write correct number of bytes")
	}

	// sync file
	err = wfd.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// make sure size hasnt changed
	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(fisize) {
		t.Fatal("size isnt correct")
	}

	// seek back to beginning
	ns, err := wfd.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if ns != 0 {
		t.Fatal("didnt seek to beginning")
	}

	// read back bytes we wrote
	buf := make([]byte, len(b))
	n, err = wfd.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(buf) {
		t.Fatal("didnt read enough")
	}

	if !bytes.Equal(buf, b) {
		t.Fatal("data read was different than data written")
	}

	// truncate file to ten bytes
	err = wfd.Truncate(10)
	if err != nil {
		t.Fatal(err)
	}

	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != 10 {
		t.Fatal("size was incorrect: ", size)
	}

	// 'writeAt' to extend it
	data := []byte("this is a test foo foo foo")
	nwa, err := wfd.WriteAt(data, 5)
	if err != nil {
		t.Fatal(err)
	}

	if nwa != len(data) {
		t.Fatal(err)
	}

	// assert size once more
	size, err = wfd.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size != int64(5+len(data)) {
		t.Fatal("size was incorrect")
	}

	// close it out!
	err = wfd.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make sure we can get node. TODO: verify it later
	_, err = fi.GetNode()
	if err != nil {
		t.Fatal(err)
	}
}

func randomWalk(d *Directory, n int) (*Directory, error) {
	for i := 0; i < n; i++ {
		dirents, err := d.List(context.Background())
		if err != nil {
			return nil, err
		}

		var childdirs []NodeListing
		for _, child := range dirents {
			if child.Type == int(TDir) {
				childdirs = append(childdirs, child)
			}
		}
		if len(childdirs) == 0 {
			return d, nil
		}

		next := childdirs[rand.Intn(len(childdirs))].Name

		nextD, err := d.Child(next)
		if err != nil {
			return nil, err
		}

		d = nextD.(*Directory)
	}
	return d, nil
}

func randomName() string {
	set := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
	length := rand.Intn(10) + 2
	var out string
	for i := 0; i < length; i++ {
		j := rand.Intn(len(set))
		out += set[j : j+1]
	}
	return out
}

func actorMakeFile(d *Directory) error {
	d, err := randomWalk(d, rand.Intn(7))
	if err != nil {
		return err
	}

	name := randomName()
	f, err := NewFile(name, dag.NodeWithData(ft.FilePBData(nil, 0)), d, d.dserv)
	if err != nil {
		return err
	}

	wfd, err := f.Open(OpenWriteOnly, true)
	if err != nil {
		return err
	}

	rread := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := io.LimitReader(rread, int64(77*rand.Intn(123)))
	_, err = io.Copy(wfd, r)
	if err != nil {
		return err
	}

	return wfd.Close()
}

func actorMkdir(d *Directory) error {
	d, err := randomWalk(d, rand.Intn(7))
	if err != nil {
		return err
	}

	_, err = d.Mkdir(randomName())

	return err
}

func randomFile(d *Directory) (*File, error) {
	d, err := randomWalk(d, rand.Intn(6))
	if err != nil {
		return nil, err
	}

	ents, err := d.List(context.Background())
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range ents {
		if e.Type == int(TFile) {
			files = append(files, e.Name)
		}
	}

	if len(files) == 0 {
		return nil, nil
	}

	fname := files[rand.Intn(len(files))]
	fsn, err := d.Child(fname)
	if err != nil {
		return nil, err
	}

	fi, ok := fsn.(*File)
	if !ok {
		return nil, errors.New("file wasnt a file, race?")
	}

	return fi, nil
}

func actorWriteFile(d *Directory) error {
	fi, err := randomFile(d)
	if err != nil {
		return err
	}
	if fi == nil {
		return nil
	}

	size := rand.Intn(1024) + 1
	buf := make([]byte, size)
	rand.Read(buf)

	s, err := fi.Size()
	if err != nil {
		return err
	}

	wfd, err := fi.Open(OpenWriteOnly, true)
	if err != nil {
		return err
	}

	offset := rand.Int63n(s)

	n, err := wfd.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("didnt write enough")
	}

	return wfd.Close()
}

func actorReadFile(d *Directory) error {
	fi, err := randomFile(d)
	if err != nil {
		return err
	}
	if fi == nil {
		return nil
	}

	_, err = fi.Size()
	if err != nil {
		return err
	}

	rfd, err := fi.Open(OpenReadOnly, false)
	if err != nil {
		return err
	}

	_, err = ioutil.ReadAll(rfd)
	if err != nil {
		return err
	}

	return rfd.Close()
}

func testActor(rt *Root, iterations int, errs chan error) {
	d := rt.GetValue().(*Directory)
	for i := 0; i < iterations; i++ {
		switch rand.Intn(5) {
		case 0:
			if err := actorMkdir(d); err != nil {
				errs <- err
				return
			}
		case 1, 2:
			if err := actorMakeFile(d); err != nil {
				errs <- err
				return
			}
		case 3:
			if err := actorWriteFile(d); err != nil {
				errs <- err
				return
			}
		case 4:
			if err := actorReadFile(d); err != nil {
				errs <- err
				return
			}
		}
	}
	errs <- nil
}

func TestMfsStress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	numroutines := 10

	errs := make(chan error)
	for i := 0; i < numroutines; i++ {
		go testActor(rt, 50, errs)
	}

	for i := 0; i < numroutines; i++ {
		err := <-errs
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMfsHugeDir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	for i := 0; i < 10000; i++ {
		err := Mkdir(rt, fmt.Sprintf("/dir%d", i), MkdirOpts{Mkparents: false, Flush: false})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMkdirP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	err := Mkdir(rt, "/a/b/c/d/e/f", MkdirOpts{Mkparents: true, Flush: true})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentWriteAndFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ds, rt := setupRoot(ctx, t)

	d := mkdirP(t, rt.GetValue().(*Directory), "foo/bar/baz")
	fn := fileNodeFromReader(t, ds, bytes.NewBuffer(nil))
	err := d.AddChild("file", fn)
	if err != nil {
		t.Fatal(err)
	}

	nloops := 5000

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nloops; i++ {
			err := writeFile(rt, "/foo/bar/baz/file", []byte("STUFF"))
			if err != nil {
				t.Error("file write failed: ", err)
				return
			}
		}
	}()

	for i := 0; i < nloops; i++ {
		_, err := rt.GetValue().GetNode()
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
}

func TestFlushing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, rt := setupRoot(ctx, t)

	dir := rt.GetValue().(*Directory)
	c := mkdirP(t, dir, "a/b/c")
	d := mkdirP(t, dir, "a/b/d")
	e := mkdirP(t, dir, "a/b/e")

	data := []byte("this is a test\n")
	nd1 := dag.NodeWithData(ft.FilePBData(data, uint64(len(data))))

	if err := c.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := d.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := e.AddChild("TEST", nd1); err != nil {
		t.Fatal(err)
	}
	if err := dir.AddChild("FILE", nd1); err != nil {
		t.Fatal(err)
	}

	if err := FlushPath(rt, "/a/b/c/TEST"); err != nil {
		t.Fatal(err)
	}

	if err := FlushPath(rt, "/a/b/d/TEST"); err != nil {
		t.Fatal(err)
	}

	if err := FlushPath(rt, "/a/b/e/TEST"); err != nil {
		t.Fatal(err)
	}

	if err := FlushPath(rt, "/FILE"); err != nil {
		t.Fatal(err)
	}

	rnd, err := dir.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	pbrnd, ok := rnd.(*dag.ProtoNode)
	if !ok {
		t.Fatal(dag.ErrNotProtobuf)
	}

	fsnode, err := ft.FSNodeFromBytes(pbrnd.Data())
	if err != nil {
		t.Fatal(err)
	}

	if fsnode.Type != ft.TDirectory {
		t.Fatal("root wasnt a directory")
	}

	rnk := rnd.Cid()
	exp := "QmWMVyhTuyxUrXX3ynz171jq76yY3PktfY9Bxiph7b9ikr"
	if rnk.String() != exp {
		t.Fatalf("dag looks wrong, expected %s, but got %s", exp, rnk.String())
	}
}

func readFile(rt *Root, path string, offset int64, buf []byte) error {
	n, err := Lookup(rt, path)
	if err != nil {
		return err
	}

	fi, ok := n.(*File)
	if !ok {
		return fmt.Errorf("%s was not a file", path)
	}

	fd, err := fi.Open(OpenReadOnly, false)
	if err != nil {
		return err
	}

	_, err = fd.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	nread, err := fd.Read(buf)
	if err != nil {
		return err
	}
	if nread != len(buf) {
		return fmt.Errorf("didnt read enough!")
	}

	return fd.Close()
}

func TestConcurrentReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	path := "a/b/c"
	d := mkdirP(t, rootdir, path)

	buf := make([]byte, 2048)
	rand.Read(buf)

	fi := fileNodeFromReader(t, ds, bytes.NewReader(buf))
	err := d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	nloops := 100
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(me int) {
			defer wg.Done()
			mybuf := make([]byte, len(buf))
			for j := 0; j < nloops; j++ {
				offset := rand.Intn(len(buf))
				length := rand.Intn(len(buf) - offset)

				err := readFile(rt, "/a/b/c/afile", int64(offset), mybuf[:length])
				if err != nil {
					t.Error("readfile failed: ", err)
					return
				}

				if !bytes.Equal(mybuf[:length], buf[offset:offset+length]) {
					t.Error("incorrect read!")
				}
			}
		}(i)
	}
	wg.Wait()
}

func writeFile(rt *Root, path string, data []byte) error {
	n, err := Lookup(rt, path)
	if err != nil {
		return err
	}

	fi, ok := n.(*File)
	if !ok {
		return fmt.Errorf("expected to receive a file, but didnt get one")
	}

	fd, err := fi.Open(OpenWriteOnly, true)
	if err != nil {
		return err
	}
	defer fd.Close()

	nw, err := fd.Write(data)
	if err != nil {
		return err
	}

	if nw != len(data) {
		return fmt.Errorf("wrote incorrect amount: %d != 10", nw)
	}

	return nil
}

func TestConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)

	rootdir := rt.GetValue().(*Directory)

	path := "a/b/c"
	d := mkdirP(t, rootdir, path)

	fi := fileNodeFromReader(t, ds, bytes.NewReader(make([]byte, 0)))
	err := d.AddChild("afile", fi)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	nloops := 100
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(me int) {
			defer wg.Done()
			mybuf := bytes.Repeat([]byte{byte(me)}, 10)
			for j := 0; j < nloops; j++ {
				err := writeFile(rt, "a/b/c/afile", mybuf)
				if err != nil {
					t.Error("writefile failed: ", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestFileDescriptors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds, rt := setupRoot(ctx, t)
	dir := rt.GetValue().(*Directory)

	nd := dag.NodeWithData(ft.FilePBData(nil, 0))
	fi, err := NewFile("test", nd, dir, ds)
	if err != nil {
		t.Fatal(err)
	}

	// test read only
	rfd1, err := fi.Open(OpenReadOnly, false)
	if err != nil {
		t.Fatal(err)
	}

	err = rfd1.Truncate(0)
	if err == nil {
		t.Fatal("shouldnt be able to truncate readonly fd")
	}

	_, err = rfd1.Write([]byte{})
	if err == nil {
		t.Fatal("shouldnt be able to write to readonly fd")
	}

	_, err = rfd1.Read([]byte{})
	if err != nil {
		t.Fatalf("expected to be able to read from file: %s", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// can open second readonly file descriptor
		rfd2, err := fi.Open(OpenReadOnly, false)
		if err != nil {
			t.Error(err)
			return
		}

		rfd2.Close()
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("open second file descriptor failed")
	case <-done:
	}

	if t.Failed() {
		return
	}

	// test not being able to open for write until reader are closed
	done = make(chan struct{})
	go func() {
		defer close(done)
		wfd1, err := fi.Open(OpenWriteOnly, true)
		if err != nil {
			t.Error(err)
		}

		wfd1.Close()
	}()

	select {
	case <-time.After(time.Millisecond * 200):
	case <-done:
		if t.Failed() {
			return
		}

		t.Fatal("shouldnt have been able to open file for writing")
	}

	err = rfd1.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("should have been able to open write fd after closing read fd")
	case <-done:
	}

	wfd, err := fi.Open(OpenWriteOnly, true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = wfd.Read([]byte{})
	if err == nil {
		t.Fatal("shouldnt have been able to read from write only filedescriptor")
	}

	_, err = wfd.Write([]byte{})
	if err != nil {
		t.Fatal(err)
	}
}
