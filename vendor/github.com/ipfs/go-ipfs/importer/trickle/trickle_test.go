package trickle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"testing"

	chunk "github.com/ipfs/go-ipfs/importer/chunk"
	h "github.com/ipfs/go-ipfs/importer/helpers"
	merkledag "github.com/ipfs/go-ipfs/merkledag"
	mdtest "github.com/ipfs/go-ipfs/merkledag/test"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"

	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

func buildTestDag(ds merkledag.DAGService, spl chunk.Splitter) (*merkledag.ProtoNode, error) {
	dbp := h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}

	nd, err := TrickleLayout(dbp.New(spl))
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*merkledag.ProtoNode)
	if !ok {
		return nil, merkledag.ErrNotProtobuf
	}

	return pbnd, VerifyTrickleDagStructure(pbnd, ds, dbp.Maxlinks, layerRepeat)
}

//Test where calls to read are smaller than the chunk size
func TestSizeBasedSplit(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	bs := chunk.SizeSplitterGen(512)
	testFileConsistency(t, bs, 32*512)

	bs = chunk.SizeSplitterGen(4096)
	testFileConsistency(t, bs, 32*4096)

	// Uneven offset
	testFileConsistency(t, bs, 31*4095)
}

func dup(b []byte) []byte {
	o := make([]byte, len(b))
	copy(o, b)
	return o
}

func testFileConsistency(t *testing.T, bs chunk.SplitterGen, nbytes int) {
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, bs(read))
	if err != nil {
		t.Fatal(err)
	}

	r, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuilderConsistency(t *testing.T) {
	nbytes := 100000
	buf := new(bytes.Buffer)
	io.CopyN(buf, u.NewTimeSeededRand(), int64(nbytes))
	should := dup(buf.Bytes())
	dagserv := mdtest.Mock()
	nd, err := buildTestDag(dagserv, chunk.DefaultSplitter(buf))
	if err != nil {
		t.Fatal(err)
	}
	r, err := uio.NewDagReader(context.Background(), nd, dagserv)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func arrComp(a, b []byte) error {
	if len(a) != len(b) {
		return fmt.Errorf("Arrays differ in length. %d != %d", len(a), len(b))
	}
	for i, v := range a {
		if v != b[i] {
			return fmt.Errorf("Arrays differ at index: %d", i)
		}
	}
	return nil
}

func TestIndirectBlocks(t *testing.T) {
	splitter := chunk.SizeSplitterGen(512)
	nbytes := 1024 * 1024
	buf := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(buf)

	read := bytes.NewReader(buf)

	ds := mdtest.Mock()
	dag, err := buildTestDag(ds, splitter(read))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := uio.NewDagReader(context.Background(), dag, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatal("Not equal!")
	}
}

func TestSeekingBasic(t *testing.T) {
	nbytes := int64(10 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 512))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	start := int64(4000)
	n, err := rs.Seek(start, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if n != start {
		t.Fatal("Failed to seek to correct offset")
	}

	out, err := ioutil.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should[start:])
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekToBegin(t *testing.T) {
	nbytes := int64(10 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	n, err := io.CopyN(ioutil.Discard, rs, 1024*4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatal("Copy didnt copy enough bytes")
	}

	seeked, err := rs.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != 0 {
		t.Fatal("Failed to seek to beginning")
	}

	out, err := ioutil.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekToAlmostBegin(t *testing.T) {
	nbytes := int64(10 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	n, err := io.CopyN(ioutil.Discard, rs, 1024*4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatal("Copy didnt copy enough bytes")
	}

	seeked, err := rs.Seek(1, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != 1 {
		t.Fatal("Failed to seek to almost beginning")
	}

	out, err := ioutil.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should[1:])
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekEnd(t *testing.T) {
	nbytes := int64(50 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	seeked, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != nbytes {
		t.Fatal("Failed to seek to end")
	}
}

func TestSeekEndSingleBlockFile(t *testing.T) {
	nbytes := int64(100)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 5000))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	seeked, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if seeked != nbytes {
		t.Fatal("Failed to seek to end")
	}
}

func TestSeekingStress(t *testing.T) {
	nbytes := int64(1024 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 1000))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	testbuf := make([]byte, nbytes)
	for i := 0; i < 50; i++ {
		offset := mrand.Intn(int(nbytes))
		l := int(nbytes) - offset
		n, err := rs.Seek(int64(offset), io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(offset) {
			t.Fatal("Seek failed to move to correct position")
		}

		nread, err := rs.Read(testbuf[:l])
		if err != nil {
			t.Fatal(err)
		}
		if nread != l {
			t.Fatal("Failed to read enough bytes")
		}

		err = arrComp(testbuf[:l], should[offset:offset+l])
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestSeekingConsistency(t *testing.T) {
	nbytes := int64(128 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(should)
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out := make([]byte, nbytes)

	for coff := nbytes - 4096; coff >= 0; coff -= 4096 {
		t.Log(coff)
		n, err := rs.Seek(coff, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if n != coff {
			t.Fatal("wasnt able to seek to the right position")
		}
		nread, err := rs.Read(out[coff : coff+4096])
		if err != nil {
			t.Fatal(err)
		}
		if nread != 4096 {
			t.Fatal("didnt read the correct number of bytes")
		}
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAppend(t *testing.T) {
	nbytes := int64(128 * 1024)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	// Reader for half the bytes
	read := bytes.NewReader(should[:nbytes/2])
	ds := mdtest.Mock()
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	dbp := &h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}

	r := bytes.NewReader(should[nbytes/2:])

	ctx := context.Background()
	nnode, err := TrickleAppend(ctx, nd, dbp.New(chunk.NewSizeSplitter(r, 500)))
	if err != nil {
		t.Fatal(err)
	}

	err = VerifyTrickleDagStructure(nnode, ds, dbp.Maxlinks, layerRepeat)
	if err != nil {
		t.Fatal(err)
	}

	fread, err := uio.NewDagReader(ctx, nnode, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(fread)
	if err != nil {
		t.Fatal(err)
	}

	err = arrComp(out, should)
	if err != nil {
		t.Fatal(err)
	}
}

// This test appends one byte at a time to an empty file
func TestMultipleAppends(t *testing.T) {
	ds := mdtest.Mock()

	// TODO: fix small size appends and make this number bigger
	nbytes := int64(1000)
	should := make([]byte, nbytes)
	u.NewTimeSeededRand().Read(should)

	read := bytes.NewReader(nil)
	nd, err := buildTestDag(ds, chunk.NewSizeSplitter(read, 500))
	if err != nil {
		t.Fatal(err)
	}

	dbp := &h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: 4,
	}

	spl := chunk.SizeSplitterGen(500)

	ctx := context.Background()
	for i := 0; i < len(should); i++ {

		nnode, err := TrickleAppend(ctx, nd, dbp.New(spl(bytes.NewReader(should[i:i+1]))))
		if err != nil {
			t.Fatal(err)
		}

		err = VerifyTrickleDagStructure(nnode, ds, dbp.Maxlinks, layerRepeat)
		if err != nil {
			t.Fatal(err)
		}

		fread, err := uio.NewDagReader(ctx, nnode, ds)
		if err != nil {
			t.Fatal(err)
		}

		out, err := ioutil.ReadAll(fread)
		if err != nil {
			t.Fatal(err)
		}

		err = arrComp(out, should[:i+1])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestAppendSingleBytesToEmpty(t *testing.T) {
	ds := mdtest.Mock()

	data := []byte("AB")

	nd := new(merkledag.ProtoNode)
	nd.SetData(ft.FilePBData(nil, 0))

	dbp := &h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: 4,
	}

	spl := chunk.SizeSplitterGen(500)

	ctx := context.Background()
	nnode, err := TrickleAppend(ctx, nd, dbp.New(spl(bytes.NewReader(data[:1]))))
	if err != nil {
		t.Fatal(err)
	}

	nnode, err = TrickleAppend(ctx, nnode, dbp.New(spl(bytes.NewReader(data[1:]))))
	if err != nil {
		t.Fatal(err)
	}

	fread, err := uio.NewDagReader(ctx, nnode, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(fread)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(out, data)
	err = arrComp(out, data)
	if err != nil {
		t.Fatal(err)
	}
}
