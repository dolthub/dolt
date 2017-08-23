// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build ignore

package main

type class int

const (
	_L           class = iota // LeftToRight
	_R                        // RightToLeft
	_EN                       // EuropeanNumber
	_ES                       // EuropeanSeparator
	_ET                       // EuropeanTerminator
	_AN                       // ArabicNumber
	_CS                       // CommonSeparator
	_B                        // ParagraphSeparator
	_S                        // SegmentSeparator
	_WS                       // WhiteSpace
	_ON                       // OtherNeutral
	_BN                       // BoundaryNeutral
	_NSM                      // NonspacingMark
	_AL                       // ArabicLetter
	classControl              // Control LRO - PDI

	numClass

	_LRO // LeftToRightOverride
	_RLO // RightToLeftOverride
	_LRE // LeftToRightEmbedding
	_RLE // RightToLeftEmbedding
	_PDF // PopDirectionalFormat
	_LRI // LeftToRightIsolate
	_RLI // RightToLeftIsolate
	_FSI // FirstStrongIsolate
	_PDI // PopDirectionalIsolate
)

var controlToClass = map[rune]class{
	0x202D: _LRO, // LeftToRightOverride,
	0x202E: _RLO, // RightToLeftOverride,
	0x202A: _LRE, // LeftToRightEmbedding,
	0x202B: _RLE, // RightToLeftEmbedding,
	0x202C: _PDF, // PopDirectionalFormat,
	0x2066: _LRI, // LeftToRightIsolate,
	0x2067: _RLI, // RightToLeftIsolate,
	0x2068: _FSI, // FirstStrongIsolate,
	0x2069: _PDI, // PopDirectionalIsolate,
}

// A trie entry has the following bits:
// 7..5  XOR mask for brackets
// 4     1: Bracket open, 0: Bracket close
// 3..0  class type
type entry uint8

const (
	openMask     = 0x10
	xorMaskShift = 5
)

func (e entry) isBracket() bool            { return e&0xF0 != 0 }
func (e entry) isOpen() bool               { return e&openMask != 0 }
func (e entry) reverseBracket(r rune) rune { return xorMasks[e>>xorMaskShift] ^ r }
func (e entry) class(r rune) class {
	c := class(e & 0x0F)
	if c == classControl {
		return controlToClass[r]
	}
	return c
}
