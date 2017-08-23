package temperrcatcher

import (
	"fmt"
	"testing"
	"time"
)

var (
	ErrTemp  = ErrTemporary{fmt.Errorf("ErrTemp")}
	ErrSkip  = fmt.Errorf("ErrSkip")
	ErrOther = fmt.Errorf("ErrOther")
)

func testTec(t *testing.T, c TempErrCatcher, errs map[error]bool) {
	for e, expected := range errs {
		if c.IsTemporary(e) != expected {
			t.Error("expected %s to be %v", e, expected)
		}
	}
}

func TestNil(t *testing.T) {
	var c TempErrCatcher
	testTec(t, c, map[error]bool{
		ErrTemp:  true,
		ErrSkip:  false,
		ErrOther: false,
	})
}

func TestWait(t *testing.T) {
	var c TempErrCatcher
	worked := make(chan time.Duration, 3)
	c.Wait = func(t time.Duration) {
		worked <- t
	}
	testTec(t, c, map[error]bool{
		ErrTemp:  true,
		ErrSkip:  false,
		ErrOther: false,
	})

	// should've called it once
	select {
	case <-worked:
	default:
		t.Error("did not call our Wait func")
	}

	// should've called it ONLY once
	select {
	case <-worked:
		t.Error("called our Wait func more than once")
	default:
	}
}

func TestTemporary(t *testing.T) {
	var c TempErrCatcher
	testTec(t, c, map[error]bool{
		ErrTemp:  true,
		ErrSkip:  false,
		ErrOther: false,
	})
}

func TestDoubles(t *testing.T) {
	last := time.Now()
	diff := func() time.Duration {
		now := time.Now()
		diff := now.Sub(last)
		last = now
		return diff
	}

	testDiff := func(low, hi time.Duration) {
		d := diff()
		grace := time.Duration(50 * time.Microsecond)
		if (d + grace) < low {
			t.Error("time difference is smaller than", low, d)
		}
		if (d - grace) > hi {
			t.Error("time difference is greater than", hi, d)
		}
	}

	var c TempErrCatcher
	testDiff(0, c.Start)
	c.IsTemporary(ErrTemp)
	testDiff(c.Start, 2*c.Start) // first time.
	c.IsTemporary(ErrTemp)
	testDiff(2*c.Start, 4*c.Start) // second time.
	c.IsTemporary(ErrTemp)
	testDiff(4*c.Start, 8*c.Start) // third time.
}

func TestDifferentStart(t *testing.T) {
	last := time.Now()
	diff := func() time.Duration {
		now := time.Now()
		diff := now.Sub(last)
		last = now
		return diff
	}

	testDiff := func(low, hi time.Duration) {
		d := diff()
		grace := time.Duration(50 * time.Microsecond)
		if (d + grace) < low {
			t.Error("time difference is smaller than", low, d)
		}
		if (d - grace) > hi {
			t.Error("time difference is greater than", hi, d)
		}
	}

	var c TempErrCatcher
	f := time.Millisecond
	testDiff(0, f)
	c.IsTemporary(ErrTemp)
	testDiff(f, 2*f) // first time.
	c.IsTemporary(ErrTemp)
	testDiff(2*f, 4*f) // second time.
	c.IsTemporary(ErrTemp)
	testDiff(4*f, 8*f) // third time.

	c.Reset()
	c.Start = 10 * time.Millisecond
	f = c.Start
	testDiff(0, f)
	c.IsTemporary(ErrTemp)
	testDiff(f, 2*f) // first time.
	c.IsTemporary(ErrTemp)
	testDiff(2*f, 4*f) // second time.
	c.IsTemporary(ErrTemp)
	testDiff(4*f, 8*f) // third time.
}

func TestDifferentStreaks(t *testing.T) {
	var c TempErrCatcher
	// one streak
	c.IsTemporary(ErrTemp) // 1
	c.IsTemporary(ErrTemp) // 2
	c.IsTemporary(ErrTemp) // 4
	expect := 4 * time.Millisecond
	if c.delay != expect {
		t.Error("delay should be:", expect, c.delay)
	}

	<-time.After(c.delay * 10)

	// a different streak
	c.IsTemporary(ErrTemp) // 1
	c.IsTemporary(ErrTemp) // 2
	c.IsTemporary(ErrTemp) // 4
	if c.delay != expect {
		t.Error("delay should be:", expect, c.delay)
	}
}

func TestFunc(t *testing.T) {
	var c TempErrCatcher
	c.IsTemp = func(e error) bool {
		return e == ErrSkip
	}
	testTec(t, c, map[error]bool{
		ErrTemp:  false,
		ErrSkip:  true,
		ErrOther: false,
	})
}
