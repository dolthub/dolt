package set

import "testing"

const (
	asciiDecimalNumbers = "0123456789"
)

func TestByteSet(t *testing.T) {
	asciiDecimalBS := NewByteSet([]byte(asciiDecimalNumbers))

	if !asciiDecimalBS.Contains('0') {
		t.Error("Should have 0.")
	}

	if asciiDecimalBS.Contains('a') {
		t.Error("Should not have 'a'")
	}

	if !asciiDecimalBS.ContainsAll([]byte("8675309")) {
		t.Error("Should contain all of this number string.")
	}

	if asciiDecimalBS.ContainsAll([]byte("0x1234")) {
		t.Error("Should not contain the x from this string")
	}
}
