package keccakpg

import (
	"bytes"
	"hash"
	"math/rand"
	"testing"
)

type test struct {
	length int
	input  []byte
	output []byte
}

var tests = []test{
	{
		28,
		[]byte{},
		[]byte{
			0xf7, 0x18, 0x37, 0x50, 0x2b, 0xa8, 0xe1, 0x08,
			0x37, 0xbd, 0xd8, 0xd3, 0x65, 0xad, 0xb8, 0x55,
			0x91, 0x89, 0x56, 0x02, 0xfc, 0x55, 0x2b, 0x48,
			0xb7, 0x39, 0x0a, 0xbd,
		},
	}, {
		28,
		[]byte("Keccak-224 Test Hash"),
		[]byte{
			0x30, 0x04, 0x5b, 0x34, 0x94, 0x6e, 0x1b, 0x2e,
			0x09, 0x16, 0x13, 0x36, 0x2f, 0xd2, 0x2a, 0xa0,
			0x8e, 0x2b, 0xea, 0xfe, 0xc5, 0xe8, 0xda, 0xee,
			0x42, 0xc2, 0xe6, 0x65},
	}, {
		32,
		[]byte{},
		[]byte{
			0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
			0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
			0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
			0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
		},
	}, {
		32,
		[]byte("Keccak-256 Test Hash"),
		[]byte{
			0xa8, 0xd7, 0x1b, 0x07, 0xf4, 0xaf, 0x26, 0xa4,
			0xff, 0x21, 0x02, 0x7f, 0x62, 0xff, 0x60, 0x26,
			0x7f, 0xf9, 0x55, 0xc9, 0x63, 0xf0, 0x42, 0xc4,
			0x6d, 0xa5, 0x2e, 0xe3, 0xcf, 0xaf, 0x3d, 0x3c},
	}, {
		48,
		[]byte{},
		[]byte{
			0x2c, 0x23, 0x14, 0x6a, 0x63, 0xa2, 0x9a, 0xcf,
			0x99, 0xe7, 0x3b, 0x88, 0xf8, 0xc2, 0x4e, 0xaa,
			0x7d, 0xc6, 0x0a, 0xa7, 0x71, 0x78, 0x0c, 0xcc,
			0x00, 0x6a, 0xfb, 0xfa, 0x8f, 0xe2, 0x47, 0x9b,
			0x2d, 0xd2, 0xb2, 0x13, 0x62, 0x33, 0x74, 0x41,
			0xac, 0x12, 0xb5, 0x15, 0x91, 0x19, 0x57, 0xff,
		},
	}, {
		48,
		[]byte("Keccak-384 Test Hash"),
		[]byte{
			0xe2, 0x13, 0xfd, 0x74, 0xaf, 0x0c, 0x5f, 0xf9,
			0x1b, 0x42, 0x3c, 0x8b, 0xce, 0xec, 0xd7, 0x01,
			0xf8, 0xdd, 0x64, 0xec, 0x18, 0xfd, 0x6f, 0x92,
			0x60, 0xfc, 0x9e, 0xc1, 0xed, 0xbd, 0x22, 0x30,
			0xa6, 0x90, 0x86, 0x65, 0xbc, 0xd9, 0xfb, 0xf4,
			0x1a, 0x99, 0xa1, 0x8a, 0x7d, 0x9e, 0x44, 0x6e},
	}, {
		64,
		[]byte{},
		[]byte{
			0x0e, 0xab, 0x42, 0xde, 0x4c, 0x3c, 0xeb, 0x92,
			0x35, 0xfc, 0x91, 0xac, 0xff, 0xe7, 0x46, 0xb2,
			0x9c, 0x29, 0xa8, 0xc3, 0x66, 0xb7, 0xc6, 0x0e,
			0x4e, 0x67, 0xc4, 0x66, 0xf3, 0x6a, 0x43, 0x04,
			0xc0, 0x0f, 0xa9, 0xca, 0xf9, 0xd8, 0x79, 0x76,
			0xba, 0x46, 0x9b, 0xcb, 0xe0, 0x67, 0x13, 0xb4,
			0x35, 0xf0, 0x91, 0xef, 0x27, 0x69, 0xfb, 0x16,
			0x0c, 0xda, 0xb3, 0x3d, 0x36, 0x70, 0x68, 0x0e,
		},
	}, {
		64,
		[]byte("Keccak-512 Test Hash"),
		[]byte{
			0x96, 0xee, 0x47, 0x18, 0xdc, 0xba, 0x3c, 0x74,
			0x61, 0x9b, 0xa1, 0xfa, 0x7f, 0x57, 0xdf, 0xe7,
			0x76, 0x9d, 0x3f, 0x66, 0x98, 0xa8, 0xb3, 0x3f,
			0xa1, 0x01, 0x83, 0x89, 0x70, 0xa1, 0x31, 0xe6,
			0x21, 0xcc, 0xfd, 0x05, 0xfe, 0xff, 0xbc, 0x11,
			0x80, 0xf2, 0x63, 0xc2, 0x7f, 0x1a, 0xda, 0xb4,
			0x60, 0x95, 0xd6, 0xf1, 0x25, 0x33, 0x14, 0x72,
			0x4b, 0x5c, 0xbf, 0x78, 0x28, 0x65, 0x8e, 0x6a,
		},
	},
}

func TestKeccak(t *testing.T) {
	for i := range tests {
		var h hash.Hash

		switch tests[i].length {
		case 28:
			h = New224()
		case 32:
			h = New256()
		case 48:
			h = New384()
		case 64:
			h = New512()
		default:
			panic("invalid testcase")
		}

		h.Write(tests[i].input)

		d := h.Sum(nil)
		if !bytes.Equal(d, tests[i].output) {
			t.Errorf("testcase %d: expected %x got %x", i, tests[i].output, d)
		}
	}
}

type testcase struct {
	msg       []byte
	output224 []byte
	output256 []byte
	output384 []byte
	output512 []byte
}

func TestKeccakShort224(t *testing.T) {
	for i := range tstShort {
		h := New224()
		h.Write(tstShort[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstShort[i].output224) {
			t.Errorf("testcase Short224 %d: expected %x got %x", i, tstShort[i].output224, d)
		}
	}
}

func TestKeccakShort256(t *testing.T) {
	for i := range tstShort {
		h := New256()
		h.Write(tstShort[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstShort[i].output256) {
			t.Errorf("testcase Short256 %d: expected %x got %x", i, tstShort[i].output256, d)
		}
	}
}

func TestKeccakShort384(t *testing.T) {
	for i := range tstShort {
		h := New384()
		h.Write(tstShort[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstShort[i].output384) {
			t.Errorf("testcase Short384 %d: expected %x got %x", i, tstShort[i].output384, d)
		}
	}
}

func TestKeccakShort512(t *testing.T) {
	for i := range tstShort {
		h := New512()
		h.Write(tstShort[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstShort[i].output512) {
			t.Errorf("testcase Short512 %d: expected %x got %x", i, tstShort[i].output512, d)
		}
	}
}

func TestKeccakLong224(t *testing.T) {
	for i := range tstLong {
		h := New224()
		h.Write(tstLong[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstLong[i].output224) {
			t.Errorf("testcase Long224 %d: expected %x got %x", i, tstLong[i].output224, d)
		}
	}
}

func TestKeccakLong256(t *testing.T) {
	for i := range tstLong {
		h := New256()
		h.Write(tstLong[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstLong[i].output256) {
			t.Errorf("testcase Long256 %d: expected %x got %x", i, tstLong[i].output256, d)
		}
	}
}

func TestKeccakLong384(t *testing.T) {
	for i := range tstLong {
		h := New384()
		h.Write(tstLong[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstLong[i].output384) {
			t.Errorf("testcase Long384 %d: expected %x got %x", i, tstLong[i].output384, d)
		}
	}
}

func TestKeccakLong512(t *testing.T) {
	for i := range tstLong {
		h := New512()
		h.Write(tstLong[i].msg)
		d := h.Sum(nil)
		if !bytes.Equal(d, tstLong[i].output512) {
			t.Errorf("testcase Long512 %d: expected %x got %x", i, tstLong[i].output512, d)
		}
	}
}

const BUF_SIZE = 1048576

var buf = func() []byte {
    result := make([]byte, BUF_SIZE)
    rand.Seed(0xDEADBEEF)
    for i := range result {
        result[i] = byte(rand.Int())
    }
    return result
}()

func BenchmarkKeccak224Write1MiB(b *testing.B) {
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h := New224()
        b.StartTimer()
        h.Write(buf)
    }
}

func BenchmarkKeccak224Sum(b *testing.B) {
    b.StopTimer()
    h := New224()
    h.Write(buf)
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h0 := h
        b.StartTimer()
        h0.Sum(nil)
    }
}

func BenchmarkKeccak224Real(b *testing.B) {
    b.StopTimer()
    b.SetBytes(int64(b.N*8))
    bs := make([]byte, 8, 8)
    h := New224()
    b.StartTimer()
    for i := 0; i < b.N; i++ {
    	h.Reset()
		bs[0], bs[1], bs[2], bs[3], bs[4], bs[5], bs[6], bs[7] = byte(i), byte(i>>8), byte(i>>16), byte(i>>24), byte(i>>32), byte(i>>40), byte(i>>48), byte(i>>56)
		h.Write(bs)
        h.Sum(nil)
    }
}

func BenchmarkKeccak256Write1MiB(b *testing.B) {
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h := New224()
        b.StartTimer()
        h.Write(buf)
    }
}

func BenchmarkKeccak256Sum(b *testing.B) {
    b.StopTimer()
    h := New256()
    h.Write(buf)
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h1 := h
        b.StartTimer()
        h1.Sum(nil)
    }
}

func BenchmarkKeccak384Write1MiB(b *testing.B) {
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h := New384()
        b.StartTimer()
        h.Write(buf)
    }
}

func BenchmarkKeccak384Sum(b *testing.B) {
    b.StopTimer()
    h := New384()
    h.Write(buf)
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h0 := h
        b.StartTimer()
        h0.Sum(nil)
    }
}

func BenchmarkKeccak512Write1MiB(b *testing.B) {
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h := New512()
        b.StartTimer()
        h.Write(buf)
    }
}

func BenchmarkKeccak512Sum(b *testing.B) {
    b.StopTimer()
    h := New512()
    h.Write(buf)
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        h0 := h
        b.StartTimer()
        h0.Sum(nil)
    }
}

