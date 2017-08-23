package ringreader

import (
	"testing"
)

func TestRingReader(t *testing.T) {
	r, _ := NewReader(256)
	t.Log("buffer:", r.Buf)

	for i := 1; i < 1048576; i = i * 2 {
		buf := make([]byte, i)
		n, err := r.Read(buf)
		if err != nil {
			t.Error(err)
		}
		if n != len(buf) {
			t.Error("did not read %d bytes", n)
		}
		t.Log("read:", buf)
	}
}
