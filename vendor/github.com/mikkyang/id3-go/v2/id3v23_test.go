// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v2

import (
	"bytes"
	"testing"
)

func TestV23Frame(t *testing.T) {
	textData := []byte{84, 80, 69, 49, 0, 0, 0, 13, 0, 0, 0, 77, 105, 99, 104, 97, 101, 108, 32, 89, 97, 110, 103}
	frame := ParseV23Frame(bytes.NewReader(textData))
	textFrame, ok := frame.(*TextFrame)
	if !ok {
		t.Errorf("ParseV23Frame on text data returns wrong type")
	}

	const text = "Michael Yang"
	if ft := textFrame.Text(); ft != text {
		t.Errorf("ParseV23Frame incorrect text, expected %s not %s", text, ft)
	}

	const encoding = "ISO-8859-1"
	if e := textFrame.Encoding(); e != encoding {
		t.Errorf("ParseV23Frame incorrect encoding, expected %s not %s", encoding, e)
	}

	if b := V23Bytes(frame); !bytes.Equal(textData, b) {
		t.Errorf("V23Bytes produces different byte slice, expected %v not %v", textData, b)
	}
}
