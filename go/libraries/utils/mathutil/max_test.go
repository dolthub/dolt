package mathutil

import "testing"

func TestMax(t *testing.T) {
	if MaxInt(1, 2) != 2 || MaxInt(2, 1) != 2 {
		t.Error("MaxInt error")
	}
	if Max(1, 2) != 2 || Max(2, 1) != 2 {
		t.Error("Max error")
	}
	if MaxInt64(1, 2) != 2 || MaxInt64(2, 1) != 2 {
		t.Error("MaxInt64 error")
	}
	if MaxFloat(1, 2) != 2 || MaxFloat(2, 1) != 2 {
		t.Error("MaxFloat error")
	}
	if MaxFloat64(1, 2) != 2 || MaxFloat64(2, 1) != 2 {
		t.Error("MaxFloat64 error")
	}
	if MaxUint(1, 2) != 2 || MaxUint(2, 1) != 2 {
		t.Error("MaxUint error")
	}
	if MaxUint64(1, 2) != 2 || MaxUint64(2, 1) != 2 {
		t.Error("MaxUint error")
	}
}

func TestMin(t *testing.T) {
	if MinInt(1, 2) != 1 || MinInt(2, 1) != 1 {
		t.Error("MinInt error")
	}
	if Min(1, 2) != 1 || Min(2, 1) != 1 {
		t.Error("Min error")
	}
	if MinInt64(1, 2) != 1 || MinInt64(2, 1) != 1 {
		t.Error("MinInt64 error")
	}
	if MinFloat(1, 2) != 1 || MinFloat(2, 1) != 1 {
		t.Error("MinFloat error")
	}
	if MinFloat64(1, 2) != 1 || MinFloat64(2, 1) != 1 {
		t.Error("MinFloat64 error")
	}
	if MinUint(1, 2) != 1 || MinUint(2, 1) != 1 {
		t.Error("MaxUint error")
	}
	if MinUint64(1, 2) != 1 || MinUint64(2, 1) != 1 {
		t.Error("MaxUint error")
	}
}
