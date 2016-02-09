package mxj

import (
	"testing"
)

func TestRemove(t *testing.T) {
	m := map[string]interface{}{
		"Div": map[string]interface{}{
			"Colour": "blue",
		},
	}
	mv := Map(m)
	err := mv.Remove("Div.Colour")
	if err != nil {
		t.Fatal(err)
	}
	if mv.Exists("Div.Colour") {
		t.Fatal("removed key still remain")
	}
}
