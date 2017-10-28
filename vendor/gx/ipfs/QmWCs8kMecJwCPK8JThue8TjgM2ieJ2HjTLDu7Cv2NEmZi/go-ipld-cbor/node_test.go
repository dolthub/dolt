package cbornode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	blocks "gx/ipfs/QmSn9Td7xgxm9EV7iEjTckpUWmWApggzPxu7eFGWkkpwin/go-block-format"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

func assertCid(c *cid.Cid, exp string) error {
	if c.String() != exp {
		return fmt.Errorf("expected cid of %s, got %s", exp, c)
	}
	return nil
}

func TestNonObject(t *testing.T) {
	nd, err := WrapObject("", mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertCid(nd.Cid(), "zdpuAuvdvGBYa3apsrf63GU9RZcrf5EBwvb82pHjUTyecbvD8"); err != nil {
		t.Fatal(err)
	}

	back, err := Decode(nd.Copy().RawData(), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(back.Cid(), "zdpuAuvdvGBYa3apsrf63GU9RZcrf5EBwvb82pHjUTyecbvD8"); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeInto(t *testing.T) {
	nd, err := WrapObject(map[string]string{
		"name": "foo",
	}, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]string
	err = DecodeInto(nd.RawData(), &m)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m["name"] != "foo" {
		t.Fatal("failed to decode object")
	}
}

func TestDecodeIntoNonObject(t *testing.T) {
	nd, err := WrapObject("foobar", mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	var s string
	err = DecodeInto(nd.RawData(), &s)
	if err != nil {
		t.Fatal(err)
	}
	if s != "foobar" {
		t.Fatal("strings don't match")
	}
}

func TestBasicMarshal(t *testing.T) {
	c := cid.NewCidV0(u.Hash([]byte("something")))

	obj := map[string]interface{}{
		"name": "foo",
		"bar":  c,
	}

	nd, err := WrapObject(obj, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(nd.Cid(), "zdpuApUZEHofKXuTs2Yv2CLBeiASQrc9FojFLSZWcyZq6dZhb"); err != nil {
		t.Fatal(err)
	}

	back, err := Decode(nd.RawData(), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(back.Cid(), "zdpuApUZEHofKXuTs2Yv2CLBeiASQrc9FojFLSZWcyZq6dZhb"); err != nil {
		t.Fatal(err)
	}

	lnk, _, err := back.ResolveLink([]string{"bar"})
	if err != nil {
		t.Fatal(err)
	}

	if !lnk.Cid.Equals(c) {
		t.Fatal("expected cid to match")
	}

	if !nd.Cid().Equals(back.Cid()) {
		t.Fatal("re-serialize failed to generate same cid")
	}
}

func TestMarshalRoundtrip(t *testing.T) {
	c1 := cid.NewCidV0(u.Hash([]byte("something1")))
	c2 := cid.NewCidV0(u.Hash([]byte("something2")))
	c3 := cid.NewCidV0(u.Hash([]byte("something3")))

	obj := map[interface{}]interface{}{
		"foo": "bar",
		"baz": []interface{}{
			c1,
			c2,
		},
		"cats": map[string]interface{}{
			"qux": c3,
		},
	}

	nd1, err := WrapObject(obj, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(nd1.Cid(), "zdpuAwc5bPhfHGdA4rs3qKzr3QB3Fjp3D16C8BRRyWzTPpY9R"); err != nil {
		t.Fatal(err)
	}

	if len(nd1.Links()) != 3 {
		t.Fatal("didnt have enough links")
	}

	nd2, err := Decode(nd1.RawData(), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if !nd1.Cid().Equals(nd2.Cid()) {
		t.Fatal("objects didnt match between marshalings")
	}

	lnk, rest, err := nd2.ResolveLink([]string{"baz", "1", "bop"})
	if err != nil {
		t.Fatal(err)
	}

	if !lnk.Cid.Equals(c2) {
		t.Fatal("expected c2")
	}

	if len(rest) != 1 || rest[0] != "bop" {
		t.Fatal("should have had one path element remaning after resolve")
	}

	out, err := nd1.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(out))
}

func assertStringsEqual(t *testing.T, a, b []string) {
	if len(a) != len(b) {
		t.Fatal("lengths differed: ", a, b)
	}

	sort.Strings(a)
	sort.Strings(b)

	for i, v := range a {
		if v != b[i] {
			t.Fatal("got mismatch: ", a, b)
		}
	}
}

func TestTree(t *testing.T) {
	c1 := cid.NewCidV0(u.Hash([]byte("something1")))
	c2 := cid.NewCidV0(u.Hash([]byte("something2")))
	c3 := cid.NewCidV0(u.Hash([]byte("something3")))
	c4 := cid.NewCidV0(u.Hash([]byte("something4")))

	obj := map[string]interface{}{
		"foo": c1,
		"baz": []interface{}{c2, c3, "c"},
		"cats": map[interface{}]interface{}{
			"qux": map[string]interface{}{
				"boo": 1,
				"baa": c4,
				"bee": 3,
				"bii": 4,
				"buu": map[interface{}]string{
					"coat": "rain",
				},
			},
		},
	}

	nd, err := WrapObject(obj, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertCid(nd.Cid(), "zdpuAqobkonFx9i79VEDiz2WcU2dC1YU8ApEVRwSC8sx5cjUP"); err != nil {
		t.Fatal(err)
	}

	full := []string{
		"foo",
		"baz",
		"baz/0",
		"baz/1",
		"baz/2",
		"cats",
		"cats/qux",
		"cats/qux/boo",
		"cats/qux/baa",
		"cats/qux/bee",
		"cats/qux/bii",
		"cats/qux/buu",
		"cats/qux/buu/coat",
	}

	assertStringsEqual(t, full, nd.Tree("", -1))

	cats := []string{
		"qux",
		"qux/boo",
		"qux/baa",
		"qux/bee",
		"qux/bii",
		"qux/buu",
		"qux/buu/coat",
	}

	assertStringsEqual(t, cats, nd.Tree("cats", -1))

	toplevel := []string{
		"foo",
		"baz",
		"cats",
	}

	assertStringsEqual(t, toplevel, nd.Tree("", 1))
	assertStringsEqual(t, []string{}, nd.Tree("", 0))
}

func TestParsing(t *testing.T) {
	b := []byte("\xd9\x01\x02\x58\x25\xa5\x03\x22\x12\x20\x65\x96\x50\xfc\x34\x43\xc9\x16\x42\x80\x48\xef\xc5\xba\x45\x58\xdc\x86\x35\x94\x98\x0a\x59\xf5\xcb\x3c\x4d\x84\x86\x7e\x6d\x31")

	n, err := Decode(b, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(n.Cid(), "zdpuApUpLTkn3YYUeuMjWToYn8nt4KQY9kQqd9uL6vwHxXnQN"); err != nil {
		t.Fatal(err)
	}

	n, err = Decode(b, mh.SHA2_512, -1)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertCid(n.Cid(), "zBwW8WMJocqnuegmghC9MyTw26Ywsdp8KTPUKkhNrnefa1X3RoNtvCCJ6kLbur2bS6TNriRb5SyFKLq9jpwtra9Fsxdd9"); err != nil {
		t.Fatal(err)
	}
}

func TestFromJson(t *testing.T) {
	data := `{
        "something": {"/":"zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw"},
        "cats": "not cats",
        "cheese": [
                {"/":"zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw"},
                {"/":"zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw"},
                {"/":"zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw"},
                {"/":"zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw"}
        ]
}`
	n, err := FromJson(bytes.NewReader([]byte(data)), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertCid(n.Cid(), "zdpuAqdmDwJ7oDv9cD4hp3yjXgWe9yhZDzRaFbRPin1c4Dz1y"); err != nil {
		t.Fatal(err)
	}

	c, ok := n.obj.(map[interface{}]interface{})["something"].(*cid.Cid)
	if !ok {
		t.Fatal("expected a cid")
	}

	if c.String() != "zb2rhisguzLFRJaxg6W3SiToBYgESFRGk1wiCRGJYF9jqk1Uw" {
		t.Fatal("cid unmarshaled wrong")
	}
}

func TestResolvedValIsJsonable(t *testing.T) {
	data := `{
		"foo": {
			"bar": 1,
			"baz": 2
		}
	}`
	n, err := FromJson(strings.NewReader(data), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertCid(n.Cid(), "zdpuAku712jAPTQBrP58frxKxeAcVZZczXNCMwBPcKJJDZWdn"); err != nil {
		t.Fatal(err)
	}

	val, _, err := n.Resolve([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}

	out, err := json.Marshal(val)
	if err != nil {
		t.Fatal(err)
	}

	if string(out) != `{"bar":1,"baz":2}` {
		t.Fatal("failed to get expected json")
	}
}

func TestExamples(t *testing.T) {
	examples := map[string]string{
		"[null]":                        "zdpuAzexuLRNr1owELqyN3ofh6yWVVKDq5wjFfmVDFbeXBHdj",
		"[]":                            "zdpuAtQy7GSHNcZxdBfmtowdL1d2WAFjJBwb6WAEfFJ6T4Gbi",
		"{}":                            "zdpuAyTBnYSugBZhqJuLsNpzjmAjSmxDqBbtAqXMtsvxiN2v3",
		"null":                          "zdpuAxKCBsAKQpEw456S49oVDkWJ9PZa44KGRfVBWHiXN3UH8",
		"1":                             "zdpuB2pwLskBDu5PZE2sepLyc3SRFPFgVXmnpzXVtWgam25kY",
		"[1]":                           "zdpuB31oq9uvbqcSTySbWhD9NMBJDjsUXKtyQNhFAsYNbYH95",
		"true":                          "zdpuAo6JPKbsmgmtujhh7mGywsAwPRmtyAYZBPKYYRjyLujD1",
		`{"a":"IPFS"}`:                  "zdpuB3AZ71ccMjBB9atM97R4wSaCYjGyztnHnjUu93t4B2XqY",
		`{"a":"IPFS","b":null,"c":[1]}`: "zdpuAyoYWNEe6xcGhkYk2SUfc7Rtbk4GkmZCrNAAnpft4Mmj5",
		`{"a":[]}`:                      "zdpuAmMgJUCDGT4WhHAych8XpSVKQXEwsWhzQhhssr8542KXw",
	}
	for originalJson, expcid := range examples {
		n, err := FromJson(bytes.NewReader([]byte(originalJson)), mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		if err := assertCid(n.Cid(), expcid); err != nil {
			t.Fatalf("for object %s: %s", originalJson, err)
		}

		cbor := n.RawData()
		node, err := Decode(cbor, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}

		node, err = Decode(cbor, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}

		jsonBytes, err := node.MarshalJSON()
		json := string(jsonBytes)
		if json != originalJson {
			t.Fatal("marshaled to incorrect JSON: " + json)
		}
	}
}

func TestCanonicalize(t *testing.T) {
	b, err := ioutil.ReadFile("test_objects/non-canon.cbor")
	if err != nil {
		t.Fatal(err)
	}
	nd1, err := Decode(b, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(b, nd1.RawData()) {
		t.Fatal("failed to canonicalize node")
	}

	if err := assertCid(nd1.Cid(), "zdpuAmxF8q6iTUtkB3xtEYzmc5Sw762qwQJftt5iW8NTWLtjC"); err != nil {
		t.Fatal(err)
	}

	nd2, err := Decode(nd1.RawData(), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	if !nd2.Cid().Equals(nd1.Cid()) || !bytes.Equal(nd2.RawData(), nd1.RawData()) {
		t.Fatal("re-decoding a canonical node should be idempotent")
	}
}

func TestStableCID(t *testing.T) {
	b, err := ioutil.ReadFile("test_objects/non-canon.cbor")
	if err != nil {
		t.Fatal(err)
	}

	hash, err := mh.Sum(b, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	c := cid.NewCidV1(cid.DagCBOR, hash)

	badBlock, err := blocks.NewBlockWithCid(b, c)
	if err != nil {
		t.Fatal(err)
	}
	badNode, err := DecodeBlock(badBlock)
	if err != nil {
		t.Fatal(err)
	}

	if !badBlock.Cid().Equals(badNode.Cid()) {
		t.Fatal("CIDs not stable")
	}
}
