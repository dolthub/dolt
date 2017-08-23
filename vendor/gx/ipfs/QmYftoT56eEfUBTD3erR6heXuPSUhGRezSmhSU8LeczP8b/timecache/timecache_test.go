package timecache

import (
	"fmt"
	"testing"
	"time"
)

func TestEntriesFound(t *testing.T) {
	tc := NewTimeCache(time.Minute)

	tc.Add("test")

	if !tc.Has("test") {
		t.Fatal("should have this key")
	}
}

func TestEntriesExpire(t *testing.T) {
	tc := NewTimeCache(time.Second)
	for i := 0; i < 11; i++ {
		tc.Add(fmt.Sprint(i))
		time.Sleep(time.Millisecond * 100)
	}

	if tc.Has(fmt.Sprint(0)) {
		t.Fatal("should have dropped this from the cache already")
	}
}
