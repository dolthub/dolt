package random

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

func TestPseudoRandom(t *testing.T) {

	var testCases = []int{
		1024,
		187654,
		1048576,
		4932132,
	}

	for _, size := range testCases {
		var buf bytes.Buffer
		err := WritePseudoRandomBytes(int64(size), &buf, int64(time.Now().UnixNano()))
		if err != nil {
			t.Fatal(err)
		}

		if buf.Len() != size {
			t.Fatal("buffer not of the right size: %d != %d", buf.Len(), size)
		}
	}
}

func TestPseudoRandomSeed(t *testing.T) {

	var first []byte
	var size = int64(1024 * 4)
	seed := time.Now().UnixNano()

	for i := 0; i < 100; i++ {
		var bufs bytes.Buffer
		var bufr bytes.Buffer

		if err := WritePseudoRandomBytes(size, &bufs, seed); err != nil {
			t.Fatal(err)
		}

		if err := WritePseudoRandomBytes(size, &bufr, time.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}

		if bufs.Len() != int(size) {
			t.Fatal("buffer not of the right size: %d != %d", bufs.Len(), size)
		}
		if bufr.Len() != int(size) {
			t.Fatal("buffer not of the right size: %d != %d", bufr.Len(), size)
		}

		if first == nil {
			first = bufs.Bytes()
		} else if !bytes.Equal(first, bufs.Bytes()) {
			t.Fatal("seeded constructed different bytes")
		}

		if bytes.Equal(first, bufr.Bytes()) {
			t.Fatal("non-seeded constructed same bytes")
		}
	}
}

func TestCryptoRandom(t *testing.T) {

	var testCases = []int{
		1024,
		187654,
		1048576,
	}

	for _, size := range testCases {
		var buf bytes.Buffer
		err := WriteRandomBytes(int64(size), &buf)
		if err != nil {
			t.Fatal(err)
		}

		if buf.Len() != size {
			t.Fatal("buffer not of the right size: %d != %d", buf.Len(), size)
		}
	}
}

func BenchmarkCryptoRandom(b *testing.B) {
	WriteRandomBytes(int64(b.N), ioutil.Discard)
}

func BenchmarkPseudoRandom(b *testing.B) {
	WritePseudoRandomBytes(int64(b.N), ioutil.Discard, time.Now().UnixNano())
}
