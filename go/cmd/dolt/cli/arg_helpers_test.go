package cli

import (
	"flag"
	"testing"
)

func initBFMTest(args []string) *BoolFlagMap {
	fs := flag.NewFlagSet("app", flag.ExitOnError)
	bfm := NewBoolFlagMap(fs, map[string]string{
		"key1": "desc1",
		"key2": "desc2",
		"key3": "desc3",
		"key4": "desc4",
		"key5": "desc5",
	})

	fs.Parse(args)
	return bfm
}

func TestNewBoolFlagMap(t *testing.T) {
	bfm := initBFMTest([]string{"-key1", "-key3"})
	trues := bfm.GetEqualTo(true)
	falses := bfm.GetEqualTo(false)

	expectedTrues := []string{"key1", "key3"}
	if !trues.ContainsAll(expectedTrues) {
		t.Error("expected trues:", expectedTrues, "actual trues:", trues.AsSlice())
	}

	expectedFalses := []string{"key2", "key4", "key5"}
	if !falses.ContainsAll(expectedFalses) {
		t.Error("expected falses:", expectedFalses, "actual falses:", falses.AsSlice())
	}
}

func initSAMTest(args []string) *StrArgMap {
	fs := flag.NewFlagSet("app", flag.ExitOnError)
	sam := NewStrArgMap(fs, map[string]string{
		"key1": "desc1",
		"key2": "desc2",
		"key3": "desc3",
		"key4": "desc4",
		"key5": "desc5",
	})

	fs.Parse(args)
	sam.Update()

	return sam
}

func TestNewArgMap(t *testing.T) {
	sam := initSAMTest([]string{"-key1", "val1", "-key3", "val3"})
	empty := sam.GetEmpty()

	if empty.Size() != 3 || !empty.ContainsAll([]string{"key2", "key4", "key5"}) {
		t.Error("Unexpected empty set contents")
	}

	if sam.Get("key1") != "val1" {
		t.Error("Unexpected value for key1")
	}
}
