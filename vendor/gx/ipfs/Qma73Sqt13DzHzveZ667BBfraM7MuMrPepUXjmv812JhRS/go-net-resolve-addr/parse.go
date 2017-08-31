// Copyright 2012 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resolve

import ()

// Bigger than we need, not too big to worry about overflow
const big = 0xFFFFFF

const HexDigit = "0123456789abcdef"

// DTOI Decimal to integer starting at &s[i0].
// Returns number, new offset, success.
func DTOI(s string, i0 int) (n int, i int, ok bool) {
	n = 0
	for i = i0; i < len(s) && '0' <= s[i] && s[i] <= '9'; i++ {
		n = n*10 + int(s[i]-'0')
		if n >= big {
			return 0, i, false
		}
	}
	if i == i0 {
		return 0, i, false
	}
	return n, i, true
}

// XTOI Hexadecimal to integer starting at &s[i0].
// Returns number, new offset, success.
func XTOI(s string, i0 int) (n int, i int, ok bool) {
	n = 0
	for i = i0; i < len(s); i++ {
		if '0' <= s[i] && s[i] <= '9' {
			n *= 16
			n += int(s[i] - '0')
		} else if 'a' <= s[i] && s[i] <= 'f' {
			n *= 16
			n += int(s[i]-'a') + 10
		} else if 'A' <= s[i] && s[i] <= 'F' {
			n *= 16
			n += int(s[i]-'A') + 10
		} else {
			break
		}
		if n >= big {
			return 0, i, false
		}
	}
	if i == i0 {
		return 0, i, false
	}
	return n, i, true
}

// XTOI2 converts the next two hex digits of s into a byte.
// If s is longer than 2 bytes then the third byte must be e.
// If the first two bytes of s are not hex digits or the third byte
// does not match e, false is returned.
func XTOI2(s string, e byte) (byte, bool) {
	if len(s) > 2 && s[2] != e {
		return 0, false
	}
	n, ei, ok := XTOI(s[:2], 0)
	return byte(n), ok && ei == 2
}

// ITOA Integer to decimal.
func IToA(i int) string {
	var buf [30]byte
	n := len(buf)
	neg := false
	if i < 0 {
		i = -i
		neg = true
	}
	ui := uint(i)
	for ui > 0 || n == len(buf) {
		n--
		buf[n] = byte('0' + ui%10)
		ui /= 10
	}
	if neg {
		n--
		buf[n] = '-'
	}
	return string(buf[n:])
}

// ITOD Convert i to decimal string.
func ITOD(i uint) string {
	if i == 0 {
		return "0"
	}

	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; i > 0; i /= 10 {
		bp--
		b[bp] = byte(i%10) + '0'
	}

	return string(b[bp:])
}

// AppendHex converts i to a hexadecimal string. Leading zeros are not printed.
func AppendHex(dst []byte, i uint32) []byte {
	if i == 0 {
		return append(dst, '0')
	}
	for j := 7; j >= 0; j-- {
		v := i >> uint(j*4)
		if v > 0 {
			dst = append(dst, HexDigit[v&0xf])
		}
	}
	return dst
}

// Number of occurrences of b in s.
func count(s string, b byte) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			n++
		}
	}
	return n
}

// Index of rightmost occurrence of b in s.
func last(s string, b byte) int {
	i := len(s)
	for i--; i >= 0; i-- {
		if s[i] == b {
			break
		}
	}
	return i
}
