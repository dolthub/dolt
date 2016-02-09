package mxj

import (
	"testing"
)

func TestRenameKey(t *testing.T) {
	m := map[string]interface{}{
		"Div": map[string]interface{}{
			"Colour": "blue",
			"Width":  "100%",
		},
	}
	mv := Map(m)
	err := mv.RenameKey("Div.Colour", "Color")
	if err != nil {
		t.Fatal(err)
	}
	values, err := mv.ValuesForPath("Div.Color")
	if len(values) != 1 {
		t.Fatal("didn't add the new key")
	}
	if values[0] != "blue" {
		t.Fatal("value is changed")
	}
	values, err = mv.ValuesForPath("Div.Colour")
	if len(values) > 0 {
		t.Fatal("didn't removed the old key")
	}

	err = mv.RenameKey("not.existing.path", "newname")
	if err == nil {
		t.Fatal("should raise an error on a non existing path")
	}

	err = mv.RenameKey("Div.Color", "Width")
	if err == nil {
		t.Fatal("should raise an error if the newName already exists")
	}
}
