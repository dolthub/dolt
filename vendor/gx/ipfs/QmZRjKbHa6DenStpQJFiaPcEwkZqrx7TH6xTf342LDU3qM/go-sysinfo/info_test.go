package sysinfo

import (
	"testing"
)

func TestMemoryStats(t *testing.T) {
	mstat, err := MemoryInfo()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(mstat.Swap)
	t.Log(mstat.Used)
}
