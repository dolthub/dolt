package travis

import (
	"os"
	"testing"
)

func TestIsRunning(t *testing.T) {
	tr := os.Getenv("TRAVIS") == "true" && os.Getenv("CI") == "true"
	if tr != IsRunning() {
		t.Error("IsRunning() does not match TRAVIS && CI env var check")
	}
}
