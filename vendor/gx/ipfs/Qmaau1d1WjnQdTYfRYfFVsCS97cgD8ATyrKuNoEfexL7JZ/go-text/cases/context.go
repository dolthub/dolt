// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cases

import (
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/transform"
)

// A context is used for iterating over source bytes, fetching case info and
// writing to a destination buffer.
//
// Casing operations may need more than one rune of context to decide how a rune
// should be cased. Casing implementations should call checkpoint on context
// whenever it is known to be safe to return the runes processed so far.
//
// It is recommended for implementations to not allow for more than 30 case
// ignorables as lookahead (analogous to the limit in norm) and to use state if
// unbounded lookahead is needed for cased runes.
type context struct {
	dst, src []byte
	atEOF    bool

	pDst int // pDst points past the last written rune in dst.
	pSrc int // pSrc points to the start of the currently scanned rune.

	// checkpoints safe to return in Transform, where nDst <= pDst and nSrc <= pSrc.
	nDst, nSrc int
	err        error

	sz   int  // size of current rune
	info info // case information of currently scanned rune

	// State preserved across calls to Transform.
	isMidWord bool // false if next cased letter needs to be title-cased.
}

func (c *context) Reset() {
	c.isMidWord = false
}

// ret returns the return values for the Transform method. It checks whether
// there were insufficient bytes in src to complete and introduces an error
// accordingly, if necessary.
func (c *context) ret() (nDst, nSrc int, err error) {
	if c.err != nil || c.nSrc == len(c.src) {
		return c.nDst, c.nSrc, c.err
	}
	// This point is only reached by mappers if there was no short destination
	// buffer. This means that the source buffer was exhausted and that c.sz was
	// set to 0 by next.
	if c.atEOF && c.pSrc == len(c.src) {
		return c.pDst, c.pSrc, nil
	}
	return c.nDst, c.nSrc, transform.ErrShortSrc
}

// checkpoint sets the return value buffer points for Transform to the current
// positions.
func (c *context) checkpoint() {
	if c.err == nil {
		c.nDst, c.nSrc = c.pDst, c.pSrc+c.sz
	}
}

// unreadRune causes the last rune read by next to be reread on the next
// invocation of next. Only one unreadRune may be called after a call to next.
func (c *context) unreadRune() {
	c.sz = 0
}

func (c *context) next() bool {
	c.pSrc += c.sz
	if c.pSrc == len(c.src) || c.err != nil {
		c.info, c.sz = 0, 0
		return false
	}
	v, sz := trie.lookup(c.src[c.pSrc:])
	c.info, c.sz = info(v), sz
	if c.sz == 0 {
		if c.atEOF {
			// A zero size means we have an incomplete rune. If we are atEOF,
			// this means it is an illegal rune, which we will consume one
			// byte at a time.
			c.sz = 1
		} else {
			c.err = transform.ErrShortSrc
			return false
		}
	}
	return true
}

// writeBytes adds bytes to dst.
func (c *context) writeBytes(b []byte) bool {
	if len(c.dst)-c.pDst < len(b) {
		c.err = transform.ErrShortDst
		return false
	}
	// This loop is faster than using copy.
	for _, ch := range b {
		c.dst[c.pDst] = ch
		c.pDst++
	}
	return true
}

// writeString writes the given string to dst.
func (c *context) writeString(s string) bool {
	if len(c.dst)-c.pDst < len(s) {
		c.err = transform.ErrShortDst
		return false
	}
	// This loop is faster than using copy.
	for i := 0; i < len(s); i++ {
		c.dst[c.pDst] = s[i]
		c.pDst++
	}
	return true
}

// copy writes the current rune to dst.
func (c *context) copy() bool {
	return c.writeBytes(c.src[c.pSrc : c.pSrc+c.sz])
}

// copyXOR copies the current rune to dst and modifies it by applying the XOR
// pattern of the case info. It is the responsibility of the caller to ensure
// that this is a rune with a XOR pattern defined.
func (c *context) copyXOR() bool {
	if !c.copy() {
		return false
	}
	if c.info&xorIndexBit == 0 {
		// Fast path for 6-bit XOR pattern, which covers most cases.
		c.dst[c.pDst-1] ^= byte(c.info >> xorShift)
	} else {
		// Interpret XOR bits as an index.
		// TODO: test performance for unrolling this loop. Verify that we have
		// at least two bytes and at most three.
		idx := c.info >> xorShift
		for p := c.pDst - 1; ; p-- {
			c.dst[p] ^= xorData[idx]
			idx--
			if xorData[idx] == 0 {
				break
			}
		}
	}
	return true
}

// hasPrefix returns true if src[pSrc:] starts with the given string.
func (c *context) hasPrefix(s string) bool {
	b := c.src[c.pSrc:]
	if len(b) < len(s) {
		return false
	}
	for i, c := range b[:len(s)] {
		if c != s[i] {
			return false
		}
	}
	return true
}

// caseType returns an info with only the case bits, normalized to either
// cLower, cUpper, cTitle or cUncased.
func (c *context) caseType() info {
	cm := c.info & 0x7
	if cm < 4 {
		return cm
	}
	if cm >= cXORCase {
		// xor the last bit of the rune with the case type bits.
		b := c.src[c.pSrc+c.sz-1]
		return info(b&1) ^ cm&0x3
	}
	if cm == cIgnorableCased {
		return cLower
	}
	return cUncased
}
