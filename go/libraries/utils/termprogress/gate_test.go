package termprogress

import "testing"

func TestSuspend_IsNestedAndIdempotent(t *testing.T) {
	if Suspended() {
		t.Fatalf("expected not suspended at start")
	}

	resume1 := Suspend()
	if !Suspended() {
		t.Fatalf("expected suspended after first suspend")
	}

	resume2 := Suspend()
	if !Suspended() {
		t.Fatalf("expected suspended after nested suspend")
	}

	// Resume is idempotent.
	resume2()
	resume2()
	if !Suspended() {
		t.Fatalf("expected still suspended after resuming inner suspend")
	}

	resume1()
	if Suspended() {
		t.Fatalf("expected not suspended after resuming all suspends")
	}
}
