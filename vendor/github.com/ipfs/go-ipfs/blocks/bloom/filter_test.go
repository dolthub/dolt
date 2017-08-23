package bloom

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBasicFilter(t *testing.T) {
	f := BasicFilter().(*filter)

	if len(f.filter) != 2048 {
		t.Fatal("basic filter should have length 2048, has:", len(f.filter))
	}
}

func TestFilter(t *testing.T) {
	f := NewFilter(128)

	keys := [][]byte{
		[]byte("hello"),
		[]byte("fish"),
		[]byte("ipfsrocks"),
		[]byte("i want ipfs socks"),
	}

	f.Add(keys[0])
	if !f.Find(keys[0]) {
		t.Fatal("Failed to find single inserted key!")
	}

	f.Add(keys[1])
	if !f.Find(keys[1]) {
		t.Fatal("Failed to find key!")
	}

	f.Add(keys[2])
	f.Add(keys[3])

	for _, k := range keys {
		if !f.Find(k) {
			t.Fatal("Couldnt find one of three keys")
		}
	}

	if f.Find([]byte("beep boop")) {
		t.Fatal("Got false positive! Super unlikely!")
	}

	fmt.Println(f)
}

func TestMerge(t *testing.T) {

	f1 := NewFilter(128)
	f2 := NewFilter(128)

	fbork := NewFilter(32)

	_, err := f1.Merge(fbork)

	if err == nil {
		t.Fatal("Merge should fail on filters with different lengths")
	}

	b := make([]byte, 4)

	var i uint32
	for i = 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(b, i)
		f1.Add(b)
	}

	for i = 10; i < 20; i++ {
		binary.LittleEndian.PutUint32(b, i)
		f2.Add(b)
	}

	merged, _ := f1.Merge(f2)

	for i = 0; i < 20; i++ {
		binary.LittleEndian.PutUint32(b, i)

		if !merged.Find(b) {
			t.Fatal("Could not find all keys in merged filter")
		}
	}
}

func TestHamming(t *testing.T) {
	f1 := NewFilter(128)
	f2 := NewFilter(128)

	f1.Add([]byte("no collision"))
	f1.Add([]byte("collision? no!"))

	dist, _ := f1.HammingDistance(f2)

	if dist != 6 {
		t.Fatal("Should have 6 bit difference")
	}
}
