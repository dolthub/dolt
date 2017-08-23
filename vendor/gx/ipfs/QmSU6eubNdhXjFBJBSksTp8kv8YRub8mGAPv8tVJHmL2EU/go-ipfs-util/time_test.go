package util

import (
	"testing"
	"time"
)

func TestTimeFormatParseInversion(t *testing.T) {
	v, err := ParseRFC3339(FormatRFC3339(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if v.Location() != time.UTC {
		t.Fatal("Time should be UTC")
	}
}
