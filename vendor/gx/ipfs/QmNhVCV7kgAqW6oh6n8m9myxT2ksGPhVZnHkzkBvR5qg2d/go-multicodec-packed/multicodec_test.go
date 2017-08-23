package mcpacked

import (
	"bytes"
	"encoding/hex"
	"testing"
)

type TestCase struct {
	code   Code
	name   string
	input  string
	result string
}

var testCases = []TestCase{
	TestCase{0x70, "dag-pb", "68656c6c6f20776f726c64", "7068656c6c6f20776f726c64"},
	TestCase{0x90, "eth-block", "68656c6c6f20776f726c64", "900168656c6c6f20776f726c64"},
	TestCase{0x96, "eth-state-trie", "68656c6c6f20776f726c64", "960168656c6c6f20776f726c64"},
	TestCase{0x30, "multicodec", "68656c6c6f20776f726c64", "3068656c6c6f20776f726c64"},
	TestCase{0x31, "multihash", "68656c6c6f20776f726c64", "3168656c6c6f20776f726c64"},
	TestCase{0x94, "eth-tx-receipt-trie", "68656c6c6f20776f726c64", "940168656c6c6f20776f726c64"},
	TestCase{0x0, "<Unknown Multicodec>", "68656c6c6f20776f726c64", "0068656c6c6f20776f726c64"},
	TestCase{0x32, "multiaddr", "68656c6c6f20776f726c64", "3268656c6c6f20776f726c64"},
	TestCase{0x91, "eth-block-list", "68656c6c6f20776f726c64", "910168656c6c6f20776f726c64"},
	TestCase{0x60, "rlp", "68656c6c6f20776f726c64", "6068656c6c6f20776f726c64"},
	TestCase{0x63, "bencode", "68656c6c6f20776f726c64", "6368656c6c6f20776f726c64"},
	TestCase{0xc1, "zcash-tx", "68656c6c6f20776f726c64", "c10168656c6c6f20776f726c64"},
	TestCase{0x7c, "torrent-file", "68656c6c6f20776f726c64", "7c68656c6c6f20776f726c64"},
	TestCase{0x69, "git", "68656c6c6f20776f726c64", "6968656c6c6f20776f726c64"},
	TestCase{0x33, "multibase", "68656c6c6f20776f726c64", "3368656c6c6f20776f726c64"},
	TestCase{0x98, "eth-storage-trie", "68656c6c6f20776f726c64", "980168656c6c6f20776f726c64"},
	TestCase{0xc0, "zcash-block", "68656c6c6f20776f726c64", "c00168656c6c6f20776f726c64"},
	TestCase{0x55, "bin", "68656c6c6f20776f726c64", "5568656c6c6f20776f726c64"},
	TestCase{0x93, "eth-tx", "68656c6c6f20776f726c64", "930168656c6c6f20776f726c64"},
	TestCase{0x95, "eth-tx-receipt", "68656c6c6f20776f726c64", "950168656c6c6f20776f726c64"},
	TestCase{0xb1, "bitcoin-tx", "68656c6c6f20776f726c64", "b10168656c6c6f20776f726c64"},
	TestCase{0xd0, "stellar-block", "68656c6c6f20776f726c64", "d00168656c6c6f20776f726c64"},
	TestCase{0x71, "dag-cbor", "68656c6c6f20776f726c64", "7168656c6c6f20776f726c64"},
	TestCase{0xd1, "stellar-tx", "68656c6c6f20776f726c64", "d10168656c6c6f20776f726c64"},
	TestCase{0x92, "eth-tx-trie", "68656c6c6f20776f726c64", "920168656c6c6f20776f726c64"},
	TestCase{0x97, "eth-account-snapshot", "68656c6c6f20776f726c64", "970168656c6c6f20776f726c64"},
	TestCase{0xb0, "bitcoin-block", "68656c6c6f20776f726c64", "b00168656c6c6f20776f726c64"},
	TestCase{0x7b, "torrent-info", "68656c6c6f20776f726c64", "7b68656c6c6f20776f726c64"},
	TestCase{0xed, "ed25519-pub", "68656c6c6f20776f726c64", "ed0168656c6c6f20776f726c64"},
}

func TestCodecPrefix(t *testing.T) {
	for _, tc := range testCases {

		data, _ := hex.DecodeString(tc.input)
		mcdata := AddPrefix(tc.code, data)

		outc, outdata := SplitPrefix(mcdata)
		if outc != tc.code {
			t.Fatal("didnt get same codec as output")
		}

		if GetCode(mcdata) != tc.code {
			t.Fatal("GetCode returned incorrect code")
		}

		if !bytes.Equal(outdata, data) {
			t.Fatal("output data not the same as input data")
		}

		res, _ := hex.DecodeString(tc.result)
		if !bytes.Equal(mcdata, res) {
			t.Fatal("didnt get expected output")
		}

		if tc.code.String() != tc.name {
			t.Fatalf("name mismatch; wanted %v, got %v", tc.name, tc.code.String())
		}
	}
}

func TestEncodeRoundtrip(t *testing.T) {
	data := []byte("Hello World")

	mcdata := AddPrefix(DagProtobuf, data)

	outc, outdata := SplitPrefix(mcdata)
	if outc != DagProtobuf {
		t.Fatal("didnt get same codec as output")
	}

	if GetCode(mcdata) != DagProtobuf {
		t.Fatal("GetCode returned incorrect code")
	}

	if !bytes.Equal(outdata, data) {
		t.Fatal("output data not the same as input data")
	}
}

func TestStringer(t *testing.T) {
	if DagCBOR.String() != "dag-cbor" {
		t.Fatal("stringify failed")
	}

	if DagProtobuf.String() != "dag-pb" {
		t.Fatal("stringify failed")
	}

	if Code(125125).String() != UnknownMulticodecString {
		t.Fatal("expected unknown mcodec string for random value")
	}
}

func TestEdgeCases(t *testing.T) {
	c := GetCode(nil)
	if c != Unknown {
		t.Fatal("invalid buffer should return Unknown")
	}
}
