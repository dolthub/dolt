package v2

import (
	"testing"
)

func TestUnsynchTextFrameSetEncoding(t *testing.T) {
	f := NewUnsynchTextFrame(V23CommonFrame["Comments"], "Foo", "Bar")
	size := f.Size()
	expectedDiff := 11

	err := f.SetEncoding("UTF-16")
	if err != nil {
		t.Fatal(err)
	}
	newSize := f.Size()
	if int(newSize-size) != expectedDiff {
		t.Errorf("expected size to increase to %d, but it was %d", size+1, newSize)
	}

	size = newSize
	err = f.SetEncoding("ISO-8859-1")
	if err != nil {
		t.Fatal(err)
	}
	newSize = f.Size()
	if int(newSize-size) != -expectedDiff {
		t.Errorf("expected size to decrease to %d, but it was %d", size-1, newSize)
	}
}
