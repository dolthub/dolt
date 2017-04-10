// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestCodecWriteNumber(t *testing.T) {
	test := func(f float64, exp []byte) {
		w := newBinaryNomsWriter()
		w.writeNumber(Number(f))
		assert.Equal(t, exp, w.data())
	}

	// We use zigzag encoding for the signed bit. For positive n we do 2*n and for negative we do 2*-n - 1
	test(0, []byte{0, 0}) //  0 * 2 **  0

	test(1, []byte{1 * 2, 0})            //  1 * 2 **  0
	test(2, []byte{1 * 2, 1 * 2})        //  1 * 2 **  1
	test(-2, []byte{(1 * 2) - 1, 1 * 2}) // -1 * 2 **  1
	test(.5, []byte{1 * 2, 1*2 - 1})     //  1 * 2 ** -1
	test(-.5, []byte{1*2 - 1, 1*2 - 1})  // -1 * 2 ** -1
	test(.25, []byte{1 * 2, 2*2 - 1})    //  1 * 2 ** -2
	test(3, []byte{3 * 2, 0})            // 0b11 * 2 ** 0

	test(15, []byte{15 * 2, 0})     // 0b1111 * 2**0
	test(256, []byte{1 * 2, 8 * 2}) // 1 * 2*8
	test(-15, []byte{15*2 - 1, 0})  // -15 * 2*0
}

func TestCodecReadNumber(t *testing.T) {
	test := func(data []byte, exp float64) {
		r := binaryNomsReader{buff: data}
		n := r.readNumber()
		assert.Equal(t, exp, float64(n))
		assert.Equal(t, len(data), int(r.offset))
	}

	test([]byte{0, 0}, 0) //  0 * 2 **  0

	test([]byte{1 * 2, 0}, 1)           //  1 * 2 **  0
	test([]byte{1 * 2, 1 * 2}, 2)       //  1 * 2 **  1
	test([]byte{1*2 - 1, 1 + 1}, -2)    // -1 * 2 **  1
	test([]byte{1 * 2, 1*2 - 1}, .5)    //  1 * 2 ** -1
	test([]byte{1*2 - 1, 1*2 - 1}, -.5) // -1 * 2 ** -1
	test([]byte{1 * 2, 2*2 - 1}, .25)   //  1 * 2 ** -2
	test([]byte{3 * 2, 0}, 3)           // 0b11 * 2 ** 0

	test([]byte{15 * 2, 0}, 15)     // 0b1111 * 2**0
	test([]byte{1 * 2, 8 * 2}, 256) // 1 * 2*8
	test([]byte{15*2 - 1, 0}, -15)  // -15 * 2*0
}
