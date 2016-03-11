// keystolower_test.go

package mxj

import (
	"fmt"
	"testing"
)

var tolowerdata1 = []byte(`
	<doc>
		<element attr="attrValue">value</element>
	</doc>
`)

var tolowerdata2 = []byte(`
	<DOC>
		<Element attR="attrValue">value</Element>
	</DOC>
`)

func TestToLower(t *testing.T) {
	fmt.Println("\n-------------- keystolower_test.go")
	fmt.Println("\nTestToLower ...")

	CoerceKeysToLower()

	m1, err := NewMapXml(tolowerdata1)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := NewMapXml(tolowerdata2)
	if err != nil {
		t.Fatal(err)
	}

	v1, err := m1.ValuesForPath("doc.element")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := m2.ValuesForPath("doc.element")
	if err != nil {
		t.Fatal(err)
	}

	if len(v1) != len(v2) {
		t.Fatal(err, len(v1), len(v2))
	}

	m := v1[0].(map[string]interface{})
	mm := v2[0].(map[string]interface{})
	for k, v := range m {
		if vv, ok := mm[k]; !ok {
			t.Fatal("key:", k, "not in mm")
		} else if v.(string) != vv.(string) {
			t.Fatal(v.(string), "not in v2:", vv.(string))
		}
	}
}
