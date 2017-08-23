package unit

import "testing"

// and the award for most meta goes to...

func TestByteSizeUnit(t *testing.T) {
	if 1*KB != 1*1024 {
		t.Fatal(1 * KB)
	}
	if 1*MB != 1*1024*1024 {
		t.Fail()
	}
	if 1*GB != 1*1024*1024*1024 {
		t.Fail()
	}
	if 1*TB != 1*1024*1024*1024*1024 {
		t.Fail()
	}
	if 1*PB != 1*1024*1024*1024*1024*1024 {
		t.Fail()
	}
	if 1*EB != 1*1024*1024*1024*1024*1024*1024 {
		t.Fail()
	}
}
