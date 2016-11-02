// Derived from https://github.com/andybalholm/dhash
// Copyright (c) 2015, Andy Balholm
// All rights reserved.

package dhash

import (
	"fmt"
	"image"
	"strings"
)

// A Hash is a 128-bit perceptual hash.
type Hash [2]uint64

var NilHash = Hash{}

func (h Hash) String() string {
	return fmt.Sprintf("%016x%016x", h[0], h[1])
}

// Parse takes the string representation of a Hash, and returns the
// corresponding Hash value.
func Parse(s string) (h Hash, err error) {
	if len(s) != 32 {
		err = fmt.Errorf("wrong length for dhash value (%d characters; should be 32)", len(s))
		return
	}
	_, err = fmt.Fscanf(strings.NewReader(s), "%016x%016x", &h[0], &h[1])
	return
}

// New returns a hash of img. The algorithm is the difference hash from
// http://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html.
func New(img image.Image) Hash {
	bounds := img.Bounds()
	width := bounds.Max.X - bounds.Min.X
	height := bounds.Max.Y - bounds.Min.Y

	// Calculate the mean brightness of each block in an 9x9 grid.
	var blocks [9][9]int
	for i := 0; i < 9; i++ {
		left := bounds.Min.X + (width * i / 9)
		right := bounds.Min.X + (width * (i + 1) / 9)
		if right == left {
			right = left + 1
		}
		for j := 0; j < 9; j++ {
			top := bounds.Min.Y + (height * j / 9)
			bottom := bounds.Min.Y + (height * (j + 1) / 9)
			if bottom == top {
				bottom = top + 1
			}
			var total int64

			switch img := img.(type) {
			case *image.YCbCr:
				for y := top; y < bottom; y++ {
					rowStart := y * img.YStride
					for x := left; x < right; x++ {
						total += int64(img.Y[rowStart+x])
					}
				}
			default:
				for x := left; x < right; x++ {
					for y := top; y < bottom; y++ {
						r, g, b, _ := img.At(x, y).RGBA()
						total += int64(r+r+r+b+g+g+g+g) >> 3
					}
				}
			}
			blocks[i][j] = int(total / int64((right-left)*(bottom-top)))
		}
	}

	var result Hash
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if blocks[i][j] > blocks[i][j+1] {
				result[0] |= 1 << uint(i*8+j)
			}
			if blocks[i][j] > blocks[i+1][j] {
				result[1] |= 1 << uint(i*8+j)
			}
		}
	}

	return result
}

// Distance returns the number of bits different between two Hash values.
func Distance(h1, h2 Hash) int {
	return bitCount(h1[0]^h2[0]) + bitCount(h1[1]^h2[1])
}

// Count returns the number of nonzero bits in w.
// (Copied from https://github.com/andybalholm/go-bit/blob/master/funcs.go)
func bitCount(w uint64) int {
	// “Software Optimization Guide for AMD64 Processors”, Section 8.6.
	const maxw = 1<<64 - 1
	const bpw = 64

	// Compute the count for each 2-bit group.
	// Example using 16-bit word w = 00,01,10,11,00,01,10,11
	// w - (w>>1) & 01,01,01,01,01,01,01,01 = 00,01,01,10,00,01,01,10
	w -= (w >> 1) & (maxw / 3)

	// Add the count of adjacent 2-bit groups and store in 4-bit groups:
	// w & 0011,0011,0011,0011 + w>>2 & 0011,0011,0011,0011 = 0001,0011,0001,0011
	w = w&(maxw/15*3) + (w>>2)&(maxw/15*3)

	// Add the count of adjacent 4-bit groups and store in 8-bit groups:
	// (w + w>>4) & 00001111,00001111 = 00000100,00000100
	w += w >> 4
	w &= maxw / 255 * 15

	// Add all 8-bit counts with a multiplication and a shift:
	// (w * 00000001,00000001) >> 8 = 00001000
	w *= maxw / 255
	w >>= (bpw/8 - 1) * 8
	return int(w)
}
