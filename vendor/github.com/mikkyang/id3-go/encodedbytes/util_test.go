// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package encodedbytes

import (
	"bytes"
	"testing"
)

func TestSynch(t *testing.T) {
	synch := []byte{0x44, 0x7a, 0x70, 0x04}
	const synchResult = 144619524

	if result, err := SynchInt(synch); result != synchResult {
		t.Errorf("encodedbytes.SynchInt(%v) = %d with error %v, want %d", synch, result, err, synchResult)
	}
	if result := SynchBytes(synchResult); !bytes.Equal(result, synch) {
		t.Errorf("encodedbytes.SynchBytes(%d) = %v, want %v", synchResult, result, synch)
	}
}

func TestNorm(t *testing.T) {
	norm := []byte{0x0b, 0x95, 0xae, 0xb4}
	const normResult = 194358964

	if result, err := NormInt(norm); result != normResult {
		t.Errorf("encodedbytes.NormInt(%v) = %d with error %v, want %d", norm, result, err, normResult)
	}
	if result := NormBytes(normResult); !bytes.Equal(result, norm) {
		t.Errorf("encodedbytes.NormBytes(%d) = %v, want %v", normResult, result, norm)
	}
}
