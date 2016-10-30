// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fuse

import (
	"testing"
)

func TestBufferPool(t *testing.T) {
	bp := NewBufferPool()
	size := 1500
	buf := bp.AllocBuffer(uint32(size))
	if len(buf) != size {
		t.Errorf("Expected buffer of %d bytes, got %d bytes", size, len(buf))
	}
	bp.FreeBuffer(buf)
}
