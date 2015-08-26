package mxj

import "testing"

func TestExists(t *testing.T) {
	m := map[string]interface{}{
		"Div": map[string]interface{}{
			"Colour": "blue",
		},
	}
	mv := Map(m)

	if !mv.Exists("Div.Colour") {
		t.Fatal("Haven't found an existing element")
	}

	if mv.Exists("Div.Color") {
		t.Fatal("Have found a non existing element")
	}
}
