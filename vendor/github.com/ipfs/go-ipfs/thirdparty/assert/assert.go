package assert

import "testing"

func Nil(err error, t *testing.T, msgs ...string) {
	if err != nil {
		t.Fatal(msgs, "error:", err)
	}
}

func True(v bool, t *testing.T, msgs ...string) {
	if !v {
		t.Fatal(msgs)
	}
}

func False(v bool, t *testing.T, msgs ...string) {
	True(!v, t, msgs...)
}

func Err(err error, t *testing.T, msgs ...string) {
	if err == nil {
		t.Fatal(msgs, "error:", err)
	}
}
