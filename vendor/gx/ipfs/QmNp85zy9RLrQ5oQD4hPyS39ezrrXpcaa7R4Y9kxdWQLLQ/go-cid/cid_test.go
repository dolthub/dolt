package cid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
	mbase "gx/ipfs/QmafgXF3u3QSWErQoZ2URmQp5PFG384htoE7J338nS2H7T/go-multibase"
)

// Copying the "silly test" idea from
// https://github.com/multiformats/go-multihash/blob/7aa9f26a231c6f34f4e9fad52bf580fd36627285/multihash_test.go#L13
// Makes it so changing the table accidentally has to happen twice.
var tCodecs = map[uint64]string{
	Raw:                "raw",
	DagProtobuf:        "protobuf",
	DagCBOR:            "cbor",
	GitRaw:             "git-raw",
	EthBlock:           "eth-block",
	EthBlockList:       "eth-block-list",
	EthTxTrie:          "eth-tx-trie",
	EthTx:              "eth-tx",
	EthTxReceiptTrie:   "eth-tx-receipt-trie",
	EthTxReceipt:       "eth-tx-receipt",
	EthStateTrie:       "eth-state-trie",
	EthAccountSnapshot: "eth-account-snapshot",
	EthStorageTrie:     "eth-storage-trie",
	BitcoinBlock:       "bitcoin-block",
	BitcoinTx:          "bitcoin-tx",
	ZcashBlock:         "zcash-block",
	ZcashTx:            "zcash-tx",
}

func assertEqual(t *testing.T, a, b *Cid) {
	if a.codec != b.codec {
		t.Fatal("mismatch on type")
	}

	if a.version != b.version {
		t.Fatal("mismatch on version")
	}

	if !bytes.Equal(a.hash, b.hash) {
		t.Fatal("multihash mismatch")
	}
}

func TestTable(t *testing.T) {
	if len(tCodecs) != len(Codecs)-1 {
		t.Errorf("Item count mismatch in the Table of Codec. Should be %d, got %d", len(tCodecs)+1, len(Codecs))
	}

	for k, v := range tCodecs {
		if Codecs[v] != k {
			t.Errorf("Table mismatch: 0x%x %s", k, v)
		}
	}
}

// The table returns cid.DagProtobuf for "v0"
// so we test it apart
func TestTableForV0(t *testing.T) {
	if Codecs["v0"] != DagProtobuf {
		t.Error("Table mismatch: Codecs[\"v0\"] should resolve to DagProtobuf (0x70)")
	}
}

func TestBasicMarshaling(t *testing.T) {
	h, err := mh.Sum([]byte("TEST"), mh.SHA3, 4)
	if err != nil {
		t.Fatal(err)
	}

	cid := &Cid{
		codec:   7,
		version: 1,
		hash:    h,
	}

	data := cid.Bytes()

	out, err := Cast(data)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(t, cid, out)

	s := cid.String()
	out2, err := Decode(s)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(t, cid, out2)
}

func TestBasesMarshaling(t *testing.T) {
	h, err := mh.Sum([]byte("TEST"), mh.SHA3, 4)
	if err != nil {
		t.Fatal(err)
	}

	cid := &Cid{
		codec:   7,
		version: 1,
		hash:    h,
	}

	data := cid.Bytes()

	out, err := Cast(data)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual(t, cid, out)

	testBases := []mbase.Encoding{
		mbase.Base16,
		mbase.Base32,
		mbase.Base32hex,
		mbase.Base32pad,
		mbase.Base32hexPad,
		mbase.Base58BTC,
		mbase.Base58Flickr,
		mbase.Base64pad,
		mbase.Base64urlPad,
		mbase.Base64url,
		mbase.Base64,
	}

	for _, b := range testBases {
		s, err := cid.StringOfBase(b)
		if err != nil {
			t.Fatal(err)
		}

		if s[0] != byte(b) {
			t.Fatal("Invalid multibase header")
		}

		out2, err := Decode(s)
		if err != nil {
			t.Fatal(err)
		}

		assertEqual(t, cid, out2)
	}
}

func TestEmptyString(t *testing.T) {
	_, err := Decode("")
	if err == nil {
		t.Fatal("shouldnt be able to parse an empty cid")
	}
}

func TestV0Handling(t *testing.T) {
	old := "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n"

	cid, err := Decode(old)
	if err != nil {
		t.Fatal(err)
	}

	if cid.version != 0 {
		t.Fatal("should have gotten version 0 cid")
	}

	if cid.hash.B58String() != old {
		t.Fatal("marshaling roundtrip failed")
	}

	if cid.String() != old {
		t.Fatal("marshaling roundtrip failed")
	}
}

func TestV0ErrorCases(t *testing.T) {
	badb58 := "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zIII"
	_, err := Decode(badb58)
	if err == nil {
		t.Fatal("should have failed to decode that ref")
	}
}

func TestPrefixRoundtrip(t *testing.T) {
	data := []byte("this is some test content")
	hash, _ := mh.Sum(data, mh.SHA2_256, -1)
	c := NewCidV1(DagCBOR, hash)

	pref := c.Prefix()

	c2, err := pref.Sum(data)
	if err != nil {
		t.Fatal(err)
	}

	if !c.Equals(c2) {
		t.Fatal("output didnt match original")
	}

	pb := pref.Bytes()

	pref2, err := PrefixFromBytes(pb)
	if err != nil {
		t.Fatal(err)
	}

	if pref.Version != pref2.Version || pref.Codec != pref2.Codec ||
		pref.MhType != pref2.MhType || pref.MhLength != pref2.MhLength {
		t.Fatal("input prefix didnt match output")
	}
}

func Test16BytesVarint(t *testing.T) {
	data := []byte("this is some test content")
	hash, _ := mh.Sum(data, mh.SHA2_256, -1)
	c := NewCidV1(DagCBOR, hash)

	c.codec = 1 << 63
	_ = c.Bytes()
}

func TestFuzzCid(t *testing.T) {
	buf := make([]byte, 128)
	for i := 0; i < 200; i++ {
		s := rand.Intn(128)
		rand.Read(buf[:s])
		_, _ = Cast(buf[:s])
	}
}

func TestParse(t *testing.T) {
	cid, err := Parse(123)
	if err == nil {
		t.Fatalf("expected error from Parse()")
	}
	if !strings.Contains(err.Error(), "can't parse 123 as Cid") {
		t.Fatalf("expected int error, got %s", err.Error())
	}

	theHash := "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n"
	h, err := mh.FromB58String(theHash)
	if err != nil {
		t.Fatal(err)
	}

	assertions := [][]interface{}{
		[]interface{}{NewCidV0(h), theHash},
		[]interface{}{NewCidV0(h).Bytes(), theHash},
		[]interface{}{h, theHash},
		[]interface{}{theHash, theHash},
		[]interface{}{"/ipfs/" + theHash, theHash},
		[]interface{}{"https://ipfs.io/ipfs/" + theHash, theHash},
		[]interface{}{"http://localhost:8080/ipfs/" + theHash, theHash},
	}

	assert := func(arg interface{}, expected string) error {
		cid, err = Parse(arg)
		if err != nil {
			return err
		}
		if cid.version != 0 {
			return fmt.Errorf("expected version 0, got %s", string(cid.version))
		}
		actual := cid.Hash().B58String()
		if actual != expected {
			return fmt.Errorf("expected hash %s, got %s", expected, actual)
		}
		actual = cid.String()
		if actual != expected {
			return fmt.Errorf("expected string %s, got %s", expected, actual)
		}
		return nil
	}

	for _, args := range assertions {
		err := assert(args[0], args[1].(string))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestHexDecode(t *testing.T) {
	hexcid := "f015512209d8453505bdc6f269678e16b3e56c2a2948a41f2c792617cc9611ed363c95b63"
	c, err := Decode(hexcid)
	if err != nil {
		t.Fatal(err)
	}

	if c.String() != "zb2rhhFAEMepUBbGyP1k8tGfz7BSciKXP6GHuUeUsJBaK6cqG" {
		t.Fatal("hash value failed to round trip decoding from hex")
	}
}

func ExampleDecode() {
	encoded := "zb2rhhFAEMepUBbGyP1k8tGfz7BSciKXP6GHuUeUsJBaK6cqG"
	c, err := Decode(encoded)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	fmt.Println(c)
	// Output: zb2rhhFAEMepUBbGyP1k8tGfz7BSciKXP6GHuUeUsJBaK6cqG
}

func TestFromJson(t *testing.T) {
	cval := "zb2rhhFAEMepUBbGyP1k8tGfz7BSciKXP6GHuUeUsJBaK6cqG"
	jsoncid := []byte(`{"/":"` + cval + `"}`)
	var c Cid
	err := json.Unmarshal(jsoncid, &c)
	if err != nil {
		t.Fatal(err)
	}

	if c.String() != cval {
		t.Fatal("json parsing failed")
	}
}
