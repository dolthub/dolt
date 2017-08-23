package util

import "testing"

func TestFileDoesNotExist(t *testing.T) {
	t.Parallel()
	if FileExists("i would be surprised to discover that this file exists") {
		t.Fail()
	}
}
