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
