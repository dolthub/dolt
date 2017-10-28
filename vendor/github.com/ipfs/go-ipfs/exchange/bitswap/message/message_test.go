package message

import (
	"bytes"
	"testing"

	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"

	pb "github.com/ipfs/go-ipfs/exchange/bitswap/message/pb"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
)

func mkFakeCid(s string) *cid.Cid {
	return cid.NewCidV0(u.Hash([]byte(s)))
}

func TestAppendWanted(t *testing.T) {
	str := mkFakeCid("foo")
	m := New(true)
	m.AddEntry(str, 1)

	if !wantlistContains(m.ToProtoV0().GetWantlist(), str) {
		t.Fail()
	}
}

func TestNewMessageFromProto(t *testing.T) {
	str := mkFakeCid("a_key")
	protoMessage := new(pb.Message)
	protoMessage.Wantlist = new(pb.Message_Wantlist)
	protoMessage.Wantlist.Entries = []*pb.Message_Wantlist_Entry{
		{Block: proto.String(str.KeyString())},
	}
	if !wantlistContains(protoMessage.Wantlist, str) {
		t.Fail()
	}
	m, err := newMessageFromProto(*protoMessage)
	if err != nil {
		t.Fatal(err)
	}

	if !wantlistContains(m.ToProtoV0().GetWantlist(), str) {
		t.Fail()
	}
}

func TestAppendBlock(t *testing.T) {

	strs := make([]string, 2)
	strs = append(strs, "Celeritas")
	strs = append(strs, "Incendia")

	m := New(true)
	for _, str := range strs {
		block := blocks.NewBlock([]byte(str))
		m.AddBlock(block)
	}

	// assert strings are in proto message
	for _, blockbytes := range m.ToProtoV0().GetBlocks() {
		s := bytes.NewBuffer(blockbytes).String()
		if !contains(strs, s) {
			t.Fail()
		}
	}
}

func TestWantlist(t *testing.T) {
	keystrs := []*cid.Cid{mkFakeCid("foo"), mkFakeCid("bar"), mkFakeCid("baz"), mkFakeCid("bat")}
	m := New(true)
	for _, s := range keystrs {
		m.AddEntry(s, 1)
	}
	exported := m.Wantlist()

	for _, k := range exported {
		present := false
		for _, s := range keystrs {

			if s.Equals(k.Cid) {
				present = true
			}
		}
		if !present {
			t.Logf("%v isn't in original list", k.Cid)
			t.Fail()
		}
	}
}

func TestCopyProtoByValue(t *testing.T) {
	str := mkFakeCid("foo")
	m := New(true)
	protoBeforeAppend := m.ToProtoV0()
	m.AddEntry(str, 1)
	if wantlistContains(protoBeforeAppend.GetWantlist(), str) {
		t.Fail()
	}
}

func TestToNetFromNetPreservesWantList(t *testing.T) {
	original := New(true)
	original.AddEntry(mkFakeCid("M"), 1)
	original.AddEntry(mkFakeCid("B"), 1)
	original.AddEntry(mkFakeCid("D"), 1)
	original.AddEntry(mkFakeCid("T"), 1)
	original.AddEntry(mkFakeCid("F"), 1)

	buf := new(bytes.Buffer)
	if err := original.ToNetV1(buf); err != nil {
		t.Fatal(err)
	}

	copied, err := FromNet(buf)
	if err != nil {
		t.Fatal(err)
	}

	if !copied.Full() {
		t.Fatal("fullness attribute got dropped on marshal")
	}

	keys := make(map[string]bool)
	for _, k := range copied.Wantlist() {
		keys[k.Cid.KeyString()] = true
	}

	for _, k := range original.Wantlist() {
		if _, ok := keys[k.Cid.KeyString()]; !ok {
			t.Fatalf("Key Missing: \"%v\"", k)
		}
	}
}

func TestToAndFromNetMessage(t *testing.T) {

	original := New(true)
	original.AddBlock(blocks.NewBlock([]byte("W")))
	original.AddBlock(blocks.NewBlock([]byte("E")))
	original.AddBlock(blocks.NewBlock([]byte("F")))
	original.AddBlock(blocks.NewBlock([]byte("M")))

	buf := new(bytes.Buffer)
	if err := original.ToNetV1(buf); err != nil {
		t.Fatal(err)
	}

	m2, err := FromNet(buf)
	if err != nil {
		t.Fatal(err)
	}

	keys := make(map[string]bool)
	for _, b := range m2.Blocks() {
		keys[b.Cid().KeyString()] = true
	}

	for _, b := range original.Blocks() {
		if _, ok := keys[b.Cid().KeyString()]; !ok {
			t.Fail()
		}
	}
}

func wantlistContains(wantlist *pb.Message_Wantlist, c *cid.Cid) bool {
	for _, e := range wantlist.GetEntries() {
		if e.GetBlock() == c.KeyString() {
			return true
		}
	}
	return false
}

func contains(strs []string, x string) bool {
	for _, s := range strs {
		if s == x {
			return true
		}
	}
	return false
}

func TestDuplicates(t *testing.T) {
	b := blocks.NewBlock([]byte("foo"))
	msg := New(true)

	msg.AddEntry(b.Cid(), 1)
	msg.AddEntry(b.Cid(), 1)
	if len(msg.Wantlist()) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}

	msg.AddBlock(b)
	msg.AddBlock(b)
	if len(msg.Blocks()) != 1 {
		t.Fatal("Duplicate in BitSwapMessage")
	}
}
