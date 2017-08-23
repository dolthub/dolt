package msgio

import (
	"strings"
	"testing"
)

func TestReader_CrashOne(t *testing.T) {
	rc := NewReader(strings.NewReader("\x83000"))
	_, err := rc.ReadMsg()
	if err != ErrMsgTooLarge {
		t.Error("should get ErrMsgTooLarge")
		t.Log(err)
	}
}

func TestVarintReader_CrashOne(t *testing.T) {
	rc := NewVarintReader(strings.NewReader("\x9a\xf1\xed\x9a0"))
	_, err := rc.ReadMsg()
	if err != ErrMsgTooLarge {
		t.Error("should get ErrMsgTooLarge")
		t.Log(err)
	}
}
