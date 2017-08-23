// Package poly1305 implements Poly1305 one-time message authentication code
// as specified in http://cr.yp.to/mac/poly1305-20050329.pdf. This package
// provides a streaming interface, unlike "golang.org/x/crypto/poly1305".
//
// For readability, we do not gofmt this file.
package poly1305

import "hash"

const Size = 16
const BlockSize = 16

type digest struct {
	r   [5]uint32
	h   [5]uint32
	pad [4]uint32
	key *[32]byte
	x   [BlockSize]byte
	nx  int
}

// New returns a new Poly1305 authenticator using the given key.
// The key must not be reused with a different message:
// "Authenticators for two messages under the same key should
// be expected to reveal enough information to allow forgeries
// of authenticators on other messages."
// See: http://nacl.cr.yp.to/onetimeauth.html
func New(key *[32]byte) hash.Hash {
	d := new(digest)
	d.key = key
	d.Reset()
	return d
}

// Reset is needed to satisfy the hash.Hash interface, but should never be
// used to authenticate a different message under the same key.
func (d *digest) Reset() {
	k := d.key

	/* r &= 0xffffffc0ffffffc0ffffffc0fffffff */
	d.r[0] = ((uint32(k[ 0]) | uint32(k[ 1])<<8 | uint32(k[ 2])<<16 | uint32(k[ 3])<<24)     ) & 0x3ffffff
	d.r[1] = ((uint32(k[ 3]) | uint32(k[ 4])<<8 | uint32(k[ 5])<<16 | uint32(k[ 6])<<24) >> 2) & 0x3ffff03
	d.r[2] = ((uint32(k[ 6]) | uint32(k[ 7])<<8 | uint32(k[ 8])<<16 | uint32(k[ 9])<<24) >> 4) & 0x3ffc0ff
	d.r[3] = ((uint32(k[ 9]) | uint32(k[10])<<8 | uint32(k[11])<<16 | uint32(k[12])<<24) >> 6) & 0x3f03fff
	d.r[4] = ((uint32(k[12]) | uint32(k[13])<<8 | uint32(k[14])<<16 | uint32(k[15])<<24) >> 8) & 0x00fffff

	d.pad[0] = uint32(k[16]) | uint32(k[17])<<8 | uint32(k[18])<<16 | uint32(k[19])<<24
	d.pad[1] = uint32(k[20]) | uint32(k[21])<<8 | uint32(k[22])<<16 | uint32(k[23])<<24
	d.pad[2] = uint32(k[24]) | uint32(k[25])<<8 | uint32(k[26])<<16 | uint32(k[27])<<24
	d.pad[3] = uint32(k[28]) | uint32(k[29])<<8 | uint32(k[30])<<16 | uint32(k[31])<<24

	d.h[0] = 0
	d.h[1] = 0
	d.h[2] = 0
	d.h[3] = 0
	d.h[4] = 0
}

func (d *digest) Write(p []byte) (nn int, err error) {
	nn = len(p)
	if d.nx > 0 {
		n := copy(d.x[d.nx:], p)
		d.nx += n
		if d.nx == BlockSize {
			d.blocks(d.x[:], false)
			d.nx = 0
		}
		p = p[n:]
	}
	if len(p) >= BlockSize {
		n := len(p) &^ (BlockSize - 1)
		d.blocks(p[:n], false)
		p = p[n:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}
	return
}

func (d *digest) blocks(p []byte, final bool) {
	var hibit uint32
	if final {
		hibit = 0
	} else {
		hibit = 1 << 24
	}

	r0 := d.r[0]
	r1 := d.r[1]
	r2 := d.r[2]
	r3 := d.r[3]
	r4 := d.r[4]

	s1 := r1 * 5
	s2 := r2 * 5
	s3 := r3 * 5
	s4 := r4 * 5

	h0 := d.h[0]
	h1 := d.h[1]
	h2 := d.h[2]
	h3 := d.h[3]
	h4 := d.h[4]

	for len(p) >= BlockSize {
		/* h += p[i] */
		h0 += ((uint32(p[ 0]) | uint32(p[ 1])<<8 | uint32(p[ 2])<<16 | uint32(p[ 3])<<24)     ) & 0x3ffffff
		h1 += ((uint32(p[ 3]) | uint32(p[ 4])<<8 | uint32(p[ 5])<<16 | uint32(p[ 6])<<24) >> 2) & 0x3ffffff
		h2 += ((uint32(p[ 6]) | uint32(p[ 7])<<8 | uint32(p[ 8])<<16 | uint32(p[ 9])<<24) >> 4) & 0x3ffffff
		h3 += ((uint32(p[ 9]) | uint32(p[10])<<8 | uint32(p[11])<<16 | uint32(p[12])<<24) >> 6) & 0x3ffffff
		h4 += ((uint32(p[12]) | uint32(p[13])<<8 | uint32(p[14])<<16 | uint32(p[15])<<24) >> 8) | hibit

		/* h *= r */
		d0 := uint64(h0)*uint64(r0) + uint64(h1)*uint64(s4) + uint64(h2)*uint64(s3) + uint64(h3)*uint64(s2) + uint64(h4)*uint64(s1)
		d1 := uint64(h0)*uint64(r1) + uint64(h1)*uint64(r0) + uint64(h2)*uint64(s4) + uint64(h3)*uint64(s3) + uint64(h4)*uint64(s2)
		d2 := uint64(h0)*uint64(r2) + uint64(h1)*uint64(r1) + uint64(h2)*uint64(r0) + uint64(h3)*uint64(s4) + uint64(h4)*uint64(s3)
		d3 := uint64(h0)*uint64(r3) + uint64(h1)*uint64(r2) + uint64(h2)*uint64(r1) + uint64(h3)*uint64(r0) + uint64(h4)*uint64(s4)
		d4 := uint64(h0)*uint64(r4) + uint64(h1)*uint64(r3) + uint64(h2)*uint64(r2) + uint64(h3)*uint64(r1) + uint64(h4)*uint64(r0)

		/* (partial) h %= p */
		var c uint32
		                 c = uint32(d0 >> 26); h0 = uint32(d0 & 0x3ffffff);
		d1 += uint64(c); c = uint32(d1 >> 26); h1 = uint32(d1 & 0x3ffffff);
		d2 += uint64(c); c = uint32(d2 >> 26); h2 = uint32(d2 & 0x3ffffff);
		d3 += uint64(c); c = uint32(d3 >> 26); h3 = uint32(d3 & 0x3ffffff);
		d4 += uint64(c); c = uint32(d4 >> 26); h4 = uint32(d4 & 0x3ffffff);
		h0 += c * 5    ; c =       (h0 >> 26); h0 =       (h0 & 0x3ffffff);
		h1 += c

		p = p[BlockSize:]
	}

	d.h[0] = h0
	d.h[1] = h1
	d.h[2] = h2
	d.h[3] = h3
	d.h[4] = h4
}

func (d0 *digest) Sum(in []byte) []byte {
	// Make a copy of d0 so that caller can keep writing and summing.
	d := *d0

	if d.nx > 0 {
		d.x[d.nx] = 1
		for i := d.nx + 1; i < BlockSize; i++ {
			d.x[i] = 0
		}
		d.blocks(d.x[:], true)
	}

	/* fully carry h */
	h0 := d.h[0]
	h1 := d.h[1]
	h2 := d.h[2]
	h3 := d.h[3]
	h4 := d.h[4]

	var c uint32
	             c = h1 >> 26; h1 = h1 & 0x3ffffff
	h2 +=     c; c = h2 >> 26; h2 = h2 & 0x3ffffff
	h3 +=     c; c = h3 >> 26; h3 = h3 & 0x3ffffff
	h4 +=     c; c = h4 >> 26; h4 = h4 & 0x3ffffff
	h0 += c * 5; c = h0 >> 26; h0 = h0 & 0x3ffffff
	h1 +=     c

	/* compute h + -p */
	g0 := h0 + 5; c = g0 >> 26; g0 &= 0x3ffffff
	g1 := h1 + c; c = g1 >> 26; g1 &= 0x3ffffff
	g2 := h2 + c; c = g2 >> 26; g2 &= 0x3ffffff
	g3 := h3 + c; c = g3 >> 26; g3 &= 0x3ffffff
	g4 := h4 + c - (1 << 26)

	/* select h if h < p, or h + -p if h >= p */
	mask := (g4 >> 31) - 1
	g0 &= mask
	g1 &= mask
	g2 &= mask
	g3 &= mask
	g4 &= mask
	mask = ^mask
	h0 = (h0 & mask) | g0
	h1 = (h1 & mask) | g1
	h2 = (h2 & mask) | g2
	h3 = (h3 & mask) | g3
	h4 = (h4 & mask) | g4

	/* h = h % (2^128) */
	h0 = ((h0      ) | (h1 << 26)) & 0xffffffff
	h1 = ((h1 >>  6) | (h2 << 20)) & 0xffffffff
	h2 = ((h2 >> 12) | (h3 << 14)) & 0xffffffff
	h3 = ((h3 >> 18) | (h4 <<  8)) & 0xffffffff

	/* mac = (h + pad) % (2^128) */
	var f uint64
	f = uint64(h0) + uint64(d.pad[0])            ; h0 = uint32(f)
	f = uint64(h1) + uint64(d.pad[1]) + (f >> 32); h1 = uint32(f)
	f = uint64(h2) + uint64(d.pad[2]) + (f >> 32); h2 = uint32(f)
	f = uint64(h3) + uint64(d.pad[3]) + (f >> 32); h3 = uint32(f)

	var sum [Size]byte
	sum[ 0] = byte(h0); sum[ 1] = byte(h0 >> 8); sum[ 2] = byte(h0 >> 16); sum[ 3] = byte(h0 >> 24)
	sum[ 4] = byte(h1); sum[ 5] = byte(h1 >> 8); sum[ 6] = byte(h1 >> 16); sum[ 7] = byte(h1 >> 24)
	sum[ 8] = byte(h2); sum[ 9] = byte(h2 >> 8); sum[10] = byte(h2 >> 16); sum[11] = byte(h2 >> 24)
	sum[12] = byte(h3); sum[13] = byte(h3 >> 8); sum[14] = byte(h3 >> 16); sum[15] = byte(h3 >> 24)

	return append(in, sum[:]...)
}

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return BlockSize }
