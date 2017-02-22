package testutil_test

import (
	"testing"

	"github.com/attic-labs/graphql/testutil"
)

func TestSubsetSlice_Simple(t *testing.T) {

	super := []interface{}{
		"1", "2", "3",
	}
	sub := []interface{}{
		"3",
	}
	if !testutil.ContainSubsetSlice(super, sub) {
		t.Fatalf("expected slice to be subset of super, got false")
	}
}
func TestSubsetSlice_Simple_Fail(t *testing.T) {

	super := []interface{}{
		"1", "2", "3",
	}
	sub := []interface{}{
		"4",
	}
	if testutil.ContainSubsetSlice(super, sub) {
		t.Fatalf("expected slice to not be subset of super, got true")
	}
}
func TestSubsetSlice_NestedSlice(t *testing.T) {

	super := []interface{}{
		[]interface{}{
			"1", "2", "3",
		},
		[]interface{}{
			"4", "5", "6",
		},
		[]interface{}{
			"7", "8", "9",
		},
	}
	sub := []interface{}{
		[]interface{}{
			"2",
		},
		[]interface{}{
			"9",
		},
		[]interface{}{
			"5",
		},
	}
	if !testutil.ContainSubsetSlice(super, sub) {
		t.Fatalf("expected slice to be subset of super, got false")
	}
}
func TestSubsetSlice_NestedSlice_DifferentLength(t *testing.T) {

	super := []interface{}{
		[]interface{}{
			"1", "2", "3",
		},
		[]interface{}{
			"4", "5", "6",
		},
		[]interface{}{
			"7", "8", "9",
		},
	}
	sub := []interface{}{
		[]interface{}{
			"3",
		},
		[]interface{}{
			"6",
		},
	}
	if !testutil.ContainSubsetSlice(super, sub) {
		t.Fatalf("expected slice to be subset of super, got false")
	}
}
func TestSubsetSlice_NestedSlice_Fail(t *testing.T) {

	super := []interface{}{
		[]interface{}{
			"1", "2", "3",
		},
		[]interface{}{
			"4", "5", "6",
		},
		[]interface{}{
			"7", "8", "9",
		},
	}
	sub := []interface{}{
		[]interface{}{
			"3",
		},
		[]interface{}{
			"3",
		},
		[]interface{}{
			"9",
		},
	}
	if !testutil.ContainSubsetSlice(super, sub) {
		t.Fatalf("expected slice to be subset of super, got false")
	}
}

func TestSubset_Simple(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	sub := map[string]interface{}{
		"c": "3",
	}
	if !testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to be subset of super, got false")
	}

}
func TestSubset_Simple_Fail(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
	}
	sub := map[string]interface{}{
		"d": "3",
	}
	if testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to not be subset of super, got true")
	}

}
func TestSubset_NestedMap(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": map[string]interface{}{
			"aa": "11",
			"bb": "22",
			"cc": "33",
		},
	}
	sub := map[string]interface{}{
		"c": "3",
		"d": map[string]interface{}{
			"cc": "33",
		},
	}
	if !testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to be subset of super, got false")
	}
}
func TestSubset_NestedMap_Fail(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": map[string]interface{}{
			"aa": "11",
			"bb": "22",
			"cc": "33",
		},
	}
	sub := map[string]interface{}{
		"c": "3",
		"d": map[string]interface{}{
			"dd": "44",
		},
	}
	if testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to not be subset of super, got true")
	}
}
func TestSubset_NestedSlice(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": []interface{}{
			"11", "22",
		},
	}
	sub := map[string]interface{}{
		"c": "3",
		"d": []interface{}{
			"11",
		},
	}
	if !testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to be subset of super, got false")
	}
}
func TestSubset_ComplexMixed(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": map[string]interface{}{
			"aa": "11",
			"bb": "22",
			"cc": []interface{}{
				"ttt", "rrr", "sss",
			},
		},
		"e": []interface{}{
			"111", "222", "333",
		},
		"f": []interface{}{
			[]interface{}{
				"9999", "8888", "7777",
			},
			[]interface{}{
				"6666", "5555", "4444",
			},
		},
	}
	sub := map[string]interface{}{
		"c": "3",
		"d": map[string]interface{}{
			"bb": "22",
			"cc": []interface{}{
				"sss",
			},
		},
		"e": []interface{}{
			"111",
		},
		"f": []interface{}{
			[]interface{}{
				"8888", "9999",
			},
			[]interface{}{
				"4444",
			},
		},
	}
	if !testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to be subset of super, got false")
	}
}
func TestSubset_ComplexMixed_Fail(t *testing.T) {

	super := map[string]interface{}{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": map[string]interface{}{
			"aa": "11",
			"bb": "22",
			"cc": []interface{}{
				"ttt", "rrr", "sss",
			},
		},
		"e": []interface{}{
			"111", "222", "333",
		},
		"f": []interface{}{
			[]interface{}{
				"9999", "8888", "7777",
			},
			[]interface{}{
				"6666", "5555", "4444",
			},
		},
	}
	sub := map[string]interface{}{
		"c": "3",
		"d": map[string]interface{}{
			"bb": "22",
			"cc": []interface{}{
				"doesnotexist",
			},
		},
		"e": []interface{}{
			"111",
		},
		"f": []interface{}{
			[]interface{}{
				"4444",
			},
		},
	}
	if testutil.ContainSubset(super, sub) {
		t.Fatalf("expected map to not be subset of super, got true")
	}
}
