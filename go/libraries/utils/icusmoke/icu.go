// Copyright 2025 Dolthub, Inc.
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

package icu

// #cgo CXXFLAGS: -std=c++17
// #cgo !windows LDFLAGS: -licui18n -licuuc -licudata
// #cgo icu_static CPPFLAGS: -DU_STATIC_IMPLEMENTATION
// #cgo windows,icu_static LDFLAGS: -lsicuin -lsicuuc -lsicudt
// #cgo windows,!icu_static LDFLAGS: -licuin -licuuc -licudt
// #include "unicode/uregex.h"
// #include <stdlib.h>
import "C"

import (
	"runtime"
	"unicode/utf16"
	"unsafe"
)

type URegularExpression struct {
	ptr     *C.URegularExpression
	cleanup runtime.Cleanup
}

type UErrorCode C.UErrorCode

func Uregex_open(str *UCharStr, flags uint32, uerr *UErrorCode) *URegularExpression {
	res := new(URegularExpression)
	var ec C.UErrorCode
	res.ptr = C.uregex_open(str.ptr, C.int32_t(str.len), C.uint32_t(flags), nil, &ec)
	if uerr != nil {
		*uerr = UErrorCode(ec)
	}
	res.cleanup = runtime.AddCleanup(res, func(ptr *C.URegularExpression) {
		C.uregex_close(ptr)
	}, res.ptr)
	return res
}

func (re *URegularExpression) Free() {
	re.cleanup.Stop()
	C.uregex_close(re.ptr)
}

// A small wrapper around an allocated blob of memory and a populated *UChar.
type UCharStr struct {
	ptr *C.UChar
	len int
	// If cap != 0, this UCharStr owns the ptr and is responsible for deallocating it.
	cap int

	cleanup runtime.Cleanup
}

func (s *UCharStr) SetString(str string) {
	uints := utf16.Encode([]rune(str))
	sz := len(uints) + 1
	s.alloc(sz)
	var i int
	for i = 0; i < len(uints); i++ {
		*(*C.UChar)(unsafe.Pointer(uintptr(unsafe.Pointer(s.ptr)) + uintptr(i)*C.sizeof_UChar)) = C.UChar(uints[i])
	}
	*(*C.UChar)(unsafe.Pointer(uintptr(unsafe.Pointer(s.ptr)) + uintptr(i)*C.sizeof_UChar)) = C.UChar(0)
	s.len = len(uints)
}

func (s *UCharStr) GetString() string {
	codeunits := make([]uint16, s.len)
	for i := 0; i < s.len; i++ {
		codeunits[i] = uint16(*(*C.UChar)(unsafe.Pointer(uintptr(unsafe.Pointer(s.ptr)) + uintptr(i)*C.sizeof_UChar)))
	}
	return string(utf16.Decode(codeunits))
}

func (s *UCharStr) GetSubstring(start, end int) string {
	return s.slice(start, end).GetString()
}

func (s *UCharStr) alloc(sz int) {
	if sz < 64 {
		sz = 64
	}
	if sz > s.cap {
		s.Free()
		s.cap = NextPow2(sz)
		s.ptr = (*C.UChar)(C.malloc(C.size_t(s.cap * C.sizeof_UChar)))
		s.cleanup = runtime.AddCleanup(s, func(ptr *C.UChar) {
			C.free(unsafe.Pointer(ptr))
		}, s.ptr)
	}
}

func (s *UCharStr) Free() {
	if s.cap > 0 {
		s.cleanup.Stop()
		C.free(unsafe.Pointer(s.ptr))
		s.cap = 0
		s.ptr = nil
		s.len = 0
	}
}

func (s *UCharStr) slice(start, end int) *UCharStr {
	// slice never owns the storage and must not outlive the owning *UCharStr, including a SetString called on it.
	var res UCharStr
	res.len = end - start
	res.ptr = (*C.UChar)(unsafe.Pointer(uintptr(unsafe.Pointer(s.ptr)) + 2*uintptr(start)))
	return &res
}

func NextPow2(i int) int {
	ui := uint32(i)
	ui--
	ui |= ui >> 1
	ui |= ui >> 2
	ui |= ui >> 4
	ui |= ui >> 8
	ui |= ui >> 16
	ui++
	return int(ui)
}
