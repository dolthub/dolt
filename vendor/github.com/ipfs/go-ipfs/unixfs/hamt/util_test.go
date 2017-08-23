package hamt

import (
	"math/big"
	"testing"
)

func TestPopCount(t *testing.T) {
	x := big.NewInt(0)

	for i := 0; i < 50; i++ {
		x.SetBit(x, i, 1)
	}

	if popCount(x) != 50 {
		t.Fatal("expected popcount to be 50")
	}
}

func TestHashBitsEvenSizes(t *testing.T) {
	buf := []byte{255, 127, 79, 45, 116, 99, 35, 17}
	hb := hashBits{b: buf}

	for _, v := range buf {
		if hb.Next(8) != int(v) {
			t.Fatal("got wrong numbers back")
		}
	}
}

func TestHashBitsUneven(t *testing.T) {
	buf := []byte{255, 127, 79, 45, 116, 99, 35, 17}
	hb := hashBits{b: buf}

	v := hb.Next(4)
	if v != 15 {
		t.Fatal("should have gotten 15: ", v)
	}

	v = hb.Next(4)
	if v != 15 {
		t.Fatal("should have gotten 15: ", v)
	}

	if v := hb.Next(3); v != 3 {
		t.Fatalf("expected 3, but got %b", v)
	}
	if v := hb.Next(3); v != 7 {
		t.Fatalf("expected 7, but got %b", v)
	}
	if v := hb.Next(3); v != 6 {
		t.Fatalf("expected 6, but got %b", v)
	}

	if v := hb.Next(15); v != 20269 {
		t.Fatalf("expected 20269, but got %b (%d)", v, v)
	}
}
