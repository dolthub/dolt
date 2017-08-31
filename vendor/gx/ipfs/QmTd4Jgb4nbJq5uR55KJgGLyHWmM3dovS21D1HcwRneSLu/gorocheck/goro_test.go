package gorocheck

import (
	"testing"
	"time"
)

func leaky() {
	go func() {
		for {
			time.Sleep(time.Second)
		}
	}()
}

func TestParse(t *testing.T) {
	err := CheckForLeaks(nil)
	if err != nil {
		t.Fatal(err)
	}

	leaky()

	time.Sleep(time.Millisecond * 50)

	err = CheckForLeaks(nil)
	if err == nil {
		t.Fatal("Expected check to fail.")
	}
}
