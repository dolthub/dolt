package json

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMarhsalToJSON(t *testing.T) {

	rowMap := []map[string]interface{}{
		map[string]interface{}{"a": []string{"a", "b", "c"}},
		map[string]interface{}{"b": []string{"1", "2", "3"}},
	}

	expected := []byte(`{
		"rows": [
			 {
			   "a": "a",
			   "b": 1
			},
			 {
				"a": "b",
				"b": 2
			},
			{
				"a":"c",
				"b": 3
			 }
		]
	}`)

	marshaled, _ := marshalToJson(rowMap)

	fmt.Println(string(marshaled))

	if !reflect.DeepEqual(marshaled, expected) {
		t.Log("marshaling did not work")
	}
}
