package multihash

import (
	"bytes"
	"io"
	"testing"
)

func TestReader(t *testing.T) {

	var buf bytes.Buffer

	for _, tc := range testCases {
		m, err := tc.Multihash()
		if err != nil {
			t.Fatal(err)
		}

		buf.Write([]byte(m))
	}

	r := NewReader(&buf)

	for _, tc := range testCases {
		h, err := tc.Multihash()
		if err != nil {
			t.Fatal(err)
		}

		h2, err := r.ReadMultihash()
		if err != nil {
			t.Error(err)
			continue
		}

		if !bytes.Equal(h, h2) {
			t.Error("h and h2 should be equal")
		}
	}
}

func TestWriter(t *testing.T) {

	var buf bytes.Buffer
	w := NewWriter(&buf)

	for _, tc := range testCases {
		m, err := tc.Multihash()
		if err != nil {
			t.Error(err)
			continue
		}

		if err := w.WriteMultihash(m); err != nil {
			t.Error(err)
			continue
		}

		buf2 := make([]byte, len(m))
		if _, err := io.ReadFull(&buf, buf2); err != nil {
			t.Error(err)
			continue
		}

		if !bytes.Equal(m, buf2) {
			t.Error("m and buf2 should be equal")
		}
	}
}
