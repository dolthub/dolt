package hamt

import (
	"math/big"
)

// hashBits is a helper that allows the reading of the 'next n bits' as an integer.
type hashBits struct {
	b        []byte
	consumed int
}

func mkmask(n int) byte {
	return (1 << uint(n)) - 1
}

// Next returns the next 'i' bits of the hashBits value as an integer
func (hb *hashBits) Next(i int) int {
	curbi := hb.consumed / 8
	leftb := 8 - (hb.consumed % 8)

	curb := hb.b[curbi]
	if i == leftb {
		out := int(mkmask(i) & curb)
		hb.consumed += i
		return out
	} else if i < leftb {
		a := curb & mkmask(leftb) // mask out the high bits we don't want
		b := a & ^mkmask(leftb-i) // mask out the low bits we don't want
		c := b >> uint(leftb-i)   // shift whats left down
		hb.consumed += i
		return int(c)
	} else {
		out := int(mkmask(leftb) & curb)
		out <<= uint(i - leftb)
		hb.consumed += leftb
		out += hb.Next(i - leftb)
		return out
	}
}

const (
	m1  = 0x5555555555555555 //binary: 0101...
	m2  = 0x3333333333333333 //binary: 00110011..
	m4  = 0x0f0f0f0f0f0f0f0f //binary:  4 zeros,  4 ones ...
	h01 = 0x0101010101010101 //the sum of 256 to the power of 0,1,2,3...
)

// from https://en.wikipedia.org/wiki/Hamming_weight
func popCountUint64(x uint64) int {
	x -= (x >> 1) & m1             //put count of each 2 bits into those 2 bits
	x = (x & m2) + ((x >> 2) & m2) //put count of each 4 bits into those 4 bits
	x = (x + (x >> 4)) & m4        //put count of each 8 bits into those 8 bits
	return int((x * h01) >> 56)
}

func popCount(i *big.Int) int {
	var n int
	for _, v := range i.Bits() {
		n += popCountUint64(uint64(v))
	}
	return n
}
