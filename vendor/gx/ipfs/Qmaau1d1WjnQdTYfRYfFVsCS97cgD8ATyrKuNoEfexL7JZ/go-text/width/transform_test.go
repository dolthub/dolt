// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package width

import (
	"bytes"
	"testing"

	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/internal/testtext"
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/transform"
)

func foldRune(r rune) (folded rune, ok bool) {
	alt, ok := mapRunes[r]
	if ok && alt.e&tagNeedsFold != 0 {
		return alt.r, true
	}
	return r, false
}

func widenRune(r rune) (wide rune, ok bool) {
	alt, ok := mapRunes[r]
	if k := alt.e.kind(); k == EastAsianHalfwidth || k == EastAsianNarrow {
		return alt.r, true
	}
	return r, false
}

func narrowRune(r rune) (narrow rune, ok bool) {
	alt, ok := mapRunes[r]
	if k := alt.e.kind(); k == EastAsianFullwidth || k == EastAsianWide || k == EastAsianAmbiguous {
		return alt.r, true
	}
	return r, false
}

func TestFoldSingleRunes(t *testing.T) {
	for r := rune(0); r < 0x1FFFF; r++ {
		if loSurrogate <= r && r <= hiSurrogate {
			continue
		}
		x, _ := foldRune(r)
		want := string(x)
		got := Fold.String(string(r))
		if got != want {
			t.Errorf("Fold().String(%U) = %+q; want %+q", r, got, want)
		}
	}
}

func TestFold(t *testing.T) {
	for _, tc := range []struct {
		desc  string
		src   string
		nDst  int
		atEOF bool
		dst   string
		nSrc  int
		err   error
	}{{
		desc:  "empty",
		src:   "",
		dst:   "",
		nDst:  10,
		nSrc:  0,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short source 1",
		src:   "a\xc2",
		dst:   "a",
		nDst:  10,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "short source 2",
		src:   "a\xe0\x80",
		dst:   "a",
		nDst:  10,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "incomplete but terminated source 1",
		src:   "a\xc2",
		dst:   "a\xc2",
		nDst:  10,
		nSrc:  2,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "incomplete but terminated source 2",
		src:   "a\xe0\x80",
		dst:   "a\xe0\x80",
		nDst:  10,
		nSrc:  3,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "exact fit dst",
		src:   "a\uff01",
		dst:   "a!",
		nDst:  2,
		nSrc:  4,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short dst 1",
		src:   "a\uffe0",
		dst:   "a",
		nDst:  2,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortDst,
	}, {
		desc:  "short dst 2",
		src:   "不夠",
		dst:   "不",
		nDst:  3,
		nSrc:  3,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		desc:  "short dst fast path",
		src:   "fast",
		dst:   "fas",
		nDst:  3,
		nSrc:  3,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		desc:  "fast path alternation",
		src:   "fast路徑fast路徑",
		dst:   "fast路徑fast路徑",
		nDst:  20,
		nSrc:  20,
		atEOF: true,
		err:   nil,
	}} {
		b := make([]byte, tc.nDst)
		nDst, nSrc, err := Fold.Transform(b, []byte(tc.src), tc.atEOF)
		if got := string(b[:nDst]); got != tc.dst {
			t.Errorf("%s: dst was %+q; want %+q", tc.desc, got, tc.dst)
		}
		if nSrc != tc.nSrc {
			t.Errorf("%s: nSrc was %d; want %d", tc.desc, nSrc, tc.nSrc)
		}
		if err != tc.err {
			t.Errorf("%s: error was %v; want %v", tc.desc, err, tc.err)
		}
	}
}

func TestWidenSingleRunes(t *testing.T) {
	for r := rune(0); r < 0x1FFFF; r++ {
		if loSurrogate <= r && r <= hiSurrogate {
			continue
		}
		alt, _ := widenRune(r)
		want := string(alt)
		got := Widen.String(string(r))
		if got != want {
			t.Errorf("Widen().String(%U) = %+q; want %+q", r, got, want)
		}
	}
}

func TestWiden(t *testing.T) {
	for _, tc := range []struct {
		desc  string
		src   string
		nDst  int
		atEOF bool
		dst   string
		nSrc  int
		err   error
	}{{
		desc:  "empty",
		src:   "",
		dst:   "",
		nDst:  10,
		nSrc:  0,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short source 1",
		src:   "a\xc2",
		dst:   "ａ",
		nDst:  10,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "short source 2",
		src:   "a\xe0\x80",
		dst:   "ａ",
		nDst:  10,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "incomplete but terminated source 1",
		src:   "a\xc2",
		dst:   "ａ\xc2",
		nDst:  10,
		nSrc:  2,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "incomplete but terminated source 2",
		src:   "a\xe0\x80",
		dst:   "ａ\xe0\x80",
		nDst:  10,
		nSrc:  3,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "exact fit dst",
		src:   "a!",
		dst:   "ａ\uff01",
		nDst:  6,
		nSrc:  2,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short dst 1",
		src:   "a\uffe0",
		dst:   "ａ",
		nDst:  4,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortDst,
	}, {
		desc:  "short dst 2",
		src:   "不夠",
		dst:   "不",
		nDst:  3,
		nSrc:  3,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		desc:  "short dst ascii",
		src:   "ascii",
		dst:   "\uff41",
		nDst:  3,
		nSrc:  1,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		desc:  "ambiguous",
		src:   "\uffe9",
		dst:   "\u2190",
		nDst:  4,
		nSrc:  3,
		atEOF: false,
		err:   nil,
	}} {
		b := make([]byte, tc.nDst)
		nDst, nSrc, err := Widen.Transform(b, []byte(tc.src), tc.atEOF)
		if got := string(b[:nDst]); got != tc.dst {
			t.Errorf("%s: dst was %+q; want %+q", tc.desc, got, tc.dst)
		}
		if nSrc != tc.nSrc {
			t.Errorf("%s: nSrc was %d; want %d", tc.desc, nSrc, tc.nSrc)
		}
		if err != tc.err {
			t.Errorf("%s: error was %v; want %v", tc.desc, err, tc.err)
		}
	}
}

func TestNarrowSingleRunes(t *testing.T) {
	for r := rune(0); r < 0x1FFFF; r++ {
		if loSurrogate <= r && r <= hiSurrogate {
			continue
		}
		alt, _ := narrowRune(r)
		want := string(alt)
		got := Narrow.String(string(r))
		if got != want {
			t.Errorf("Narrow().String(%U) = %+q; want %+q", r, got, want)
		}
	}
}

func TestNarrow(t *testing.T) {
	for _, tc := range []struct {
		desc  string
		src   string
		nDst  int
		atEOF bool
		dst   string
		nSrc  int
		err   error
	}{{
		desc:  "empty",
		src:   "",
		dst:   "",
		nDst:  10,
		nSrc:  0,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short source 1",
		src:   "a\xc2",
		dst:   "a",
		nDst:  10,
		nSrc:  1,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "short source 2",
		src:   "ａ\xe0\x80",
		dst:   "a",
		nDst:  10,
		nSrc:  3,
		atEOF: false,
		err:   transform.ErrShortSrc,
	}, {
		desc:  "incomplete but terminated source 1",
		src:   "ａ\xc2",
		dst:   "a\xc2",
		nDst:  10,
		nSrc:  4,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "incomplete but terminated source 2",
		src:   "ａ\xe0\x80",
		dst:   "a\xe0\x80",
		nDst:  10,
		nSrc:  5,
		atEOF: true,
		err:   nil,
	}, {
		desc:  "exact fit dst",
		src:   "ａ\uff01",
		dst:   "a!",
		nDst:  2,
		nSrc:  6,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short dst 1",
		src:   "ａ\uffe0",
		dst:   "a",
		nDst:  2,
		nSrc:  3,
		atEOF: false,
		err:   transform.ErrShortDst,
	}, {
		desc:  "short dst 2",
		src:   "不夠",
		dst:   "不",
		nDst:  3,
		nSrc:  3,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		// Create a narrow variant of ambiguous runes, if they exist.
		desc:  "ambiguous",
		src:   "\u2190",
		dst:   "\uffe9",
		nDst:  4,
		nSrc:  3,
		atEOF: false,
		err:   nil,
	}, {
		desc:  "short dst fast path",
		src:   "fast",
		dst:   "fas",
		nDst:  3,
		nSrc:  3,
		atEOF: true,
		err:   transform.ErrShortDst,
	}, {
		desc:  "fast path alternation",
		src:   "fast路徑fast路徑",
		dst:   "fast路徑fast路徑",
		nDst:  20,
		nSrc:  20,
		atEOF: true,
		err:   nil,
	}} {
		b := make([]byte, tc.nDst)
		nDst, nSrc, err := Narrow.Transform(b, []byte(tc.src), tc.atEOF)
		if got := string(b[:nDst]); got != tc.dst {
			t.Errorf("%s: dst was %+q; want %+q", tc.desc, got, tc.dst)
		}
		if nSrc != tc.nSrc {
			t.Errorf("%s: nSrc was %d; want %d", tc.desc, nSrc, tc.nSrc)
		}
		if err != tc.err {
			t.Errorf("%s: error was %v; want %v", tc.desc, err, tc.err)
		}
	}
}
func bench(b *testing.B, t Transformer, s string) {
	dst := make([]byte, 1024)
	src := []byte(s)
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Transform(dst, src, true)
	}
}

func changingRunes(f func(r rune) (rune, bool)) string {
	buf := &bytes.Buffer{}
	for r := rune(0); r <= 0xFFFF; r++ {
		if _, ok := foldRune(r); ok {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func BenchmarkFoldASCII(b *testing.B) {
	bench(b, Fold, testtext.ASCII)
}

func BenchmarkFoldCJK(b *testing.B) {
	bench(b, Fold, testtext.CJK)
}

func BenchmarkFoldNonCanonical(b *testing.B) {
	bench(b, Fold, changingRunes(foldRune))
}

func BenchmarkFoldOther(b *testing.B) {
	bench(b, Fold, testtext.TwoByteUTF8+testtext.ThreeByteUTF8)
}

func BenchmarkWideASCII(b *testing.B) {
	bench(b, Widen, testtext.ASCII)
}

func BenchmarkWideCJK(b *testing.B) {
	bench(b, Widen, testtext.CJK)
}

func BenchmarkWideNonCanonical(b *testing.B) {
	bench(b, Widen, changingRunes(widenRune))
}

func BenchmarkWideOther(b *testing.B) {
	bench(b, Widen, testtext.TwoByteUTF8+testtext.ThreeByteUTF8)
}

func BenchmarkNarrowASCII(b *testing.B) {
	bench(b, Narrow, testtext.ASCII)
}

func BenchmarkNarrowCJK(b *testing.B) {
	bench(b, Narrow, testtext.CJK)
}

func BenchmarkNarrowNonCanonical(b *testing.B) {
	bench(b, Narrow, changingRunes(narrowRune))
}

func BenchmarkNarrowOther(b *testing.B) {
	bench(b, Narrow, testtext.TwoByteUTF8+testtext.ThreeByteUTF8)
}
