package mxj

import (
	"testing"
)

func TestSetValueForPath(t *testing.T) {
	m := map[string]interface{}{
		"Div": map[string]interface{}{
			"Colour": "blue",
			"Font": map[string]interface{}{
				"Family": "sans",
			},
		},
	}
	mv := Map(m)

	// testing setting a new key
	err := mv.SetValueForPath("big", "Div.Font.Size")
	if err != nil {
		t.Fatal(err)
	}
	val, err := mv.ValueForPathString("Div.Font.Size")
	if err != nil {
		t.Fatal(err)
	}
	if val != "big" {
		t.Fatal("key's value hasn't changed")
	}

	// testing setting a new value to en existing key
	err = mv.SetValueForPath("red", "Div.Colour")
	if err != nil {
		t.Fatal(err)
	}
	val, err = mv.ValueForPathString("Div.Colour")
	if err != nil {
		t.Fatal(err)
	}
	if val != "red" {
		t.Fatal("existig key's value hasn't changed")
	}
}
