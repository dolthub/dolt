package keyspace

import (
	"bytes"
	"math/big"
	"testing"
)

func TestPrefixLen(t *testing.T) {
	cases := [][]byte{
		{0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		{0x00, 0x58, 0xFF, 0x80, 0x00, 0x00, 0xF0},
	}
	lens := []int{24, 56, 9}

	for i, c := range cases {
		r := ZeroPrefixLen(c)
		if r != lens[i] {
			t.Errorf("ZeroPrefixLen failed: %v != %v", r, lens[i])
		}
	}

}

func TestXorKeySpace(t *testing.T) {

	ids := [][]byte{
		{0xFF, 0xFF, 0xFF, 0xFF},
		{0x00, 0x00, 0x00, 0x00},
		{0xFF, 0xFF, 0xFF, 0xF0},
	}

	ks := [][2]Key{
		{XORKeySpace.Key(ids[0]), XORKeySpace.Key(ids[0])},
		{XORKeySpace.Key(ids[1]), XORKeySpace.Key(ids[1])},
		{XORKeySpace.Key(ids[2]), XORKeySpace.Key(ids[2])},
	}

	for i, set := range ks {
		if !set[0].Equal(set[1]) {
			t.Errorf("Key not eq. %v != %v", set[0], set[1])
		}

		if !bytes.Equal(set[0].Bytes, set[1].Bytes) {
			t.Errorf("Key gen failed. %v != %v", set[0].Bytes, set[1].Bytes)
		}

		if !bytes.Equal(set[0].Original, ids[i]) {
			t.Errorf("ptrs to original. %v != %v", set[0].Original, ids[i])
		}

		if len(set[0].Bytes) != 32 {
			t.Errorf("key length incorrect. 32 != %d", len(set[0].Bytes))
		}
	}

	for i := 1; i < len(ks); i++ {
		if ks[i][0].Less(ks[i-1][0]) == ks[i-1][0].Less(ks[i][0]) {
			t.Errorf("less should be different.")
		}

		if ks[i][0].Distance(ks[i-1][0]).Cmp(ks[i-1][0].Distance(ks[i][0])) != 0 {
			t.Errorf("distance should be the same.")
		}

		if ks[i][0].Equal(ks[i-1][0]) {
			t.Errorf("Keys should not be eq. %v != %v", ks[i][0], ks[i-1][0])
		}
	}
}

func TestDistancesAndCenterSorting(t *testing.T) {

	adjs := [][]byte{
		{173, 149, 19, 27, 192, 183, 153, 192, 177, 175, 71, 127, 177, 79, 207, 38, 166, 169, 247, 96, 121, 228, 139, 240, 144, 172, 183, 232, 54, 123, 253, 14},
		{223, 63, 97, 152, 4, 169, 47, 219, 64, 87, 25, 45, 196, 61, 215, 72, 234, 119, 138, 220, 82, 188, 73, 140, 232, 5, 36, 192, 20, 184, 17, 25},
		{73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199, 53, 185, 58, 157, 117, 78, 52, 146, 157, 127},
		{73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199, 53, 185, 58, 157, 117, 78, 52, 146, 157, 127},
		{73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199, 53, 185, 58, 157, 117, 78, 52, 146, 157, 126},
		{73, 0, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199, 53, 185, 58, 157, 117, 78, 52, 146, 157, 127},
	}

	keys := make([]Key, len(adjs))
	for i, a := range adjs {
		keys[i] = Key{Space: XORKeySpace, Bytes: a}
	}

	cmp := func(a int64, b *big.Int) int {
		return big.NewInt(a).Cmp(b)
	}

	if 0 != cmp(0, keys[2].Distance(keys[3])) {
		t.Errorf("distance calculation wrong: %v", keys[2].Distance(keys[3]))
	}

	if 0 != cmp(1, keys[2].Distance(keys[4])) {
		t.Errorf("distance calculation wrong: %v", keys[2].Distance(keys[4]))
	}

	d1 := keys[2].Distance(keys[5])
	d2 := XOR(keys[2].Bytes, keys[5].Bytes)
	d2 = d2[len(keys[2].Bytes)-len(d1.Bytes()):] // skip empty space for big
	if !bytes.Equal(d1.Bytes(), d2) {
		t.Errorf("bytes should be the same. %v == %v", d1.Bytes(), d2)
	}

	if -1 != cmp(2<<32, keys[2].Distance(keys[5])) {
		t.Errorf("2<<32 should be smaller")
	}

	keys2 := SortByDistance(XORKeySpace, keys[2], keys)
	order := []int{2, 3, 4, 5, 1, 0}
	for i, o := range order {
		if !bytes.Equal(keys[o].Bytes, keys2[i].Bytes) {
			t.Errorf("order is wrong. %d?? %v == %v", o, keys[o], keys2[i])
		}
	}

}
