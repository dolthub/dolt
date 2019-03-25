package json

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUnmarshalFromJSON(t *testing.T) {
	testJSON := `{
		"rows": [
			 {
			   "id": 0,
			   "first name": "tim",
			   "last name": "sehn",
			   "title": "ceo",
			   "start date": "8/6/2018",
			   "end date": ""
			},
			 {
			   "id": 1,
			   "first name": "brian",
			   "last name": "hendriks",
			   "title": "software engienner",
			   "start date": "8/6/2018",
			   "end date": ""
			}
		]
	}`

	rowData, _ := UnmarshalFromJSON([]byte(testJSON))
	marshaledRowData, _ := json.Marshal(rowData)

	if rowData == nil {
		t.Error("did not unmarshal")
	}

	if reflect.DeepEqual(testJSON, marshaledRowData) {
		t.Error("something went wrong")
	}
}
