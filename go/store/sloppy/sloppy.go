// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package sloppy

import (
	"github.com/dolthub/dolt/go/store/d"
)

const (
	maxOffsetPOT = uint16(12)
	maxTableSize = 1 << 14
	maxLength    = 1<<12 - 1
	tableMask    = maxTableSize - 1
	shift        = uint32(20)
)

// TODO: Make this configurable
var maxOffset = int(1<<maxOffsetPOT - 1)

// Sloppy is a logical variant of Snappy. Its purpose to provide a kind of
// estimate of how a given byte sequence *will be* compressed by Snappy. It is
// useful when a byte stream is fed into a rolling hash with the goal of
// achieving a given average chunk byte length *after compression*. Sloppy is
// logically similar to snappy, but prefers "copies" which are closer to the
// repeated byte sequence (snappy prefers to refer to the *first* instance of a
// repeated byte sequence). This is important for mitigating the likelihood that
// altering any byte in an input stream will cause chunk boundaries to be
// redrawn downstream.
//
// The high-level approach is to maintain a logical mapping between four-byte
// sequences which have been observed in the stream so-far and the integer
// offset of observed sequence (the mapping is done with a "cheap" hash-function
// which permits false-positives because they can be trivial filtered out). In
// the non-matched state, for each new byte consumed, a uint32 is computed from
// the next 4 bytes and then a look-up is performed to check for a matching 4
// bytes earlier in the stream. Snappy and sloppy behave roughly identical thus
// far.
//
// When in the "matched state" (attempting to extend the current match), Snappy
// does not re-index new 4-byte sequences, but Sloppy does. The reason for this
// is that Sloppy would like match the most recent occurrence as it moves
// forward.
//
// Lastly, Sloppy adds two novel heuritics, both aimed at further mitigating
// the chance of chunk boundaries being redrawn because of byte value changes:
//
// 1) During the first 2 bytes of match, it *continues* to look for closer
// matches (effectively preferring a closer but shorter copy to a further but
// longer one). The reason for this is that when sequences repeat frequently in
// a byte stream, randomness provides for a good chance that a one or two byte
// prefix on a repeated sequence will match "far away". E.g.
//
// "23hello my friend, 12hello my friend, 01hello my friend, 23hello my friend"
//
// In the above sequence, sloppy would prefer to copy the final
// "hello my friend" 19 bytes backwards rather than "23hello my friend" quite a
// bit further.
//
// 2) Sloppy will only emit copies which are "worth it". I.e. The longer the
// reference back, the longer the length of the copy must be.
type Sloppy struct {
	enc                      encoder
	idx                      int
	matching                 bool
	matchOffset, matchLength int
	table                    [maxTableSize]uint32
}

// New returns a new sloppy encoder which will encode to |f|. If |f| ever
// returns false, then encoding ends immediately. |f| is a callback because
// the primary use is that the "encoded" byte stream is fed byte-by-byte
// into a rolling hash function.
func New(f func(b byte) bool) *Sloppy {
	return &Sloppy{
		binaryEncoder{f},
		0,
		false,
		0, 0,
		[maxTableSize]uint32{},
	}
}

// Update continues the encoding of a given input stream. The caller is expected
// to call update after having (ONLY) appended bytes to |src|. When |Update|
// returns, sloppy will have emitted 0 or more literals or copies by calling
// the |sf.f|. Note that sloppy will ALWAYS buffer the final three bytes of
// input.
func (sl *Sloppy) Update(src []byte) {
	// Only consume up to the point that a "look-ahead" can include 4 bytes.
	for ; sl.idx < len(src)-3; sl.idx++ {
		nextHash := fbhash(load32(src, sl.idx))

		if sl.matching && (sl.matchLength > maxLength || src[sl.idx] != src[sl.matchOffset+sl.matchLength]) {
			// End Match
			if sl.maybeCopy(src) {
				return // terminate if consumer has "closed"
			}
		}

		// Look for a match if we are beyond the first byte AND either there is no
		// match yet, OR we are matching, but fewer than 3 bytes have been
		// matched. The later condition allows for giving up to 2 bytes of a copy
		// in order to reference a "closer" sequence. Empirical tests on
		// structured data, suggests this reduces the average offset by ~2/3.
		if sl.idx > 0 && (!sl.matching || sl.matchLength < 3) {
			matchPos := int(sl.table[nextHash&tableMask])

			if sl.idx > matchPos &&
				src[sl.idx] == src[matchPos] && // filter false positives
				sl.idx-matchPos <= maxOffset && // don't refer back beyond maxOffset
				(!sl.matching || matchPos >= sl.matchOffset+4) { // if we are "rematching", ensure the new match is at least 4 bytes closer

				if sl.matching {
					// We are dropping an existing match for a closer one. Emit the
					// matched bytes as literals
					if sl.dontCopy(src, sl.idx-sl.matchLength, sl.idx) {
						return // terminate if consumer has "closed"
					}
				}

				// Begin a new match
				sl.matching = true
				sl.matchOffset = matchPos
				sl.matchLength = 0
			}
		}

		// Store new hashed offset
		sl.table[nextHash&tableMask] = uint32(sl.idx)

		if sl.matching {
			sl.matchLength++
		} else {
			if sl.enc.emitLiteral(src[sl.idx]) {
				return // terminate if consumer has "closed"
			}
		}
	}
}

func (sl *Sloppy) Reset() {
	sl.idx = 0
	sl.matching = false
	sl.matchOffset = 0
	sl.matchLength = 0
	sl.table = [maxTableSize]uint32{}
}

// len >= 2^(2 + log2(maxOffset) - log2(maxOffset-off)). IOW, for the first 1/2
// of the maxOffset, a copy must be >= 4. For 1/2 of what remains, a copy must
// be >= 8, etc...
func copyLongEnough(off, len uint16) bool {
	d.PanicIfTrue(off == 0)

	p := uint16(0)
	x := (1 << maxOffsetPOT) - off
	for x > 0 {
		x = x >> 1
		p++
	}

	i := maxOffsetPOT - p
	min := 4
	for i > 0 {
		min = min << 1
		i--
	}

	return int(len) >= min
}

// Emit matches bytes as literals.
func (sl *Sloppy) dontCopy(src []byte, from, to int) bool {
	for ; from < to; from++ {
		if sl.enc.emitLiteral(src[from]) {
			return true
		}
	}
	return false
}

// Emit a copy if the length is sufficient for a given offset
func (sl *Sloppy) maybeCopy(src []byte) bool {
	off, len := uint16(sl.idx-(sl.matchOffset+sl.matchLength)), uint16(sl.matchLength)
	sl.matching = false
	sl.matchOffset = 0
	sl.matchLength = 0

	if !copyLongEnough(off, len) {
		return sl.dontCopy(src, sl.idx-int(len), sl.idx)
	}

	return sl.enc.emitCopy(off, len)
}

type encoder interface {
	emitLiteral(b byte) bool
	emitCopy(offset, length uint16) bool
}

type binaryEncoder struct {
	f func(b byte) bool
}

func (be binaryEncoder) emitLiteral(b byte) bool {
	return be.f(b)
}

func (be binaryEncoder) emitCopy(offset, length uint16) bool {
	// all copies are encoded as 3 bytes.
	// 12 bits for offset and 12 bits for length

	// 8 MSBits of offset
	if be.f(byte(offset >> 4)) {
		return true
	}

	// 4 LSBits offset | 4 MSBits length
	if be.f(byte(offset<<4) | byte(length>>4)) {
		return true
	}

	// 8 LSBits of length
	if be.f(byte(length)) {
		return true
	}

	return false
}

func fbhash(u uint32) uint32 {
	return (u * 0x1e35a7bd) >> shift
}

func load32(b []byte, i int) uint32 {
	b = b[i : i+4 : len(b)] // Help the compiler eliminate bounds checks on the next line.
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
