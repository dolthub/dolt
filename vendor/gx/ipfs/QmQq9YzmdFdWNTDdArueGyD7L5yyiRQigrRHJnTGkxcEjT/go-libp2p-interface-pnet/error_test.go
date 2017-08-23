package ipnet

import (
	"errors"
	"testing"
)

func TestIsPnetErr(t *testing.T) {
	err := NewError("test")

	if err.Error() != "privnet: test" {
		t.Fatalf("expected 'privnet: test' got '%s'", err.Error())
	}
	if !IsPNetError(err) {
		t.Fatal("expected the pnetErr to be detected by IsPnetError")
	}
	if IsPNetError(errors.New("not pnet error")) {
		t.Fatal("expected generic error not to be pnetError")
	}
}
