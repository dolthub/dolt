package nbs

import (
	"testing"
)

func TestCompressedChunkIsEmpty(t *testing.T) {
        if !EmptyCompressedChunk.IsEmpty() {
                t.Fatal("EmptyCompressedChunkIsEmpty() should equal true.")
        }
        if !(CompressedChunk{}).IsEmpty() {
                t.Fatal("CompressedChunk{}.IsEmpty() should equal true.")
        }
}
