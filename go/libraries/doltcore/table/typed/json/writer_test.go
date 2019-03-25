package json

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMarhsalToJSON(t *testing.T) {

	rowMap := map[string]interface{}{

		"id":         0,
		"first name": "tim",
	}

	expected := []byte(`{"first name": "tim", "id": 0}`)

	marshaled, _ := marshalToJson(rowMap)

	fmt.Println(string(marshaled))

	if !reflect.DeepEqual(marshaled, expected) {
		t.Log("marshaling did not work")
	}
}
