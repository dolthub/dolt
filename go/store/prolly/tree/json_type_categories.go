// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
)

type jsonTypeCategory int

const (
	jsonTypeNull jsonTypeCategory = iota
	jsonTypeNumber
	jsonTypeString
	jsonTypeObject
	jsonTypeArray
	jsonTypeBoolean
)

func getTypeCategoryOfValue(val interface{}) (jsonTypeCategory, error) {
	if val == nil {
		return jsonTypeNull, nil
	}
	switch val.(type) {
	case map[string]interface{}:
		return jsonTypeObject, nil
	case []interface{}:
		return jsonTypeArray, nil
	case bool:
		return jsonTypeBoolean, nil
	case string:
		return jsonTypeString, nil
	case decimal.Decimal, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		return jsonTypeNumber, nil
	}
	return 0, fmt.Errorf("expected json value, got %v", val)
}

// getTypeCategoryFromFirstCharacter returns the type of a JSON object by inspecting its first byte.
func getTypeCategoryFromFirstCharacter(c byte) jsonTypeCategory {
	switch c {
	case '{':
		return jsonTypeObject
	case '[':
		return jsonTypeArray
	case 'n':
		return jsonTypeNull
	case 't', 'f':
		return jsonTypeBoolean
	case '"':
		return jsonTypeString
	default:
		return jsonTypeNumber
	}
}

func IsJsonObject(json sql.JSONWrapper) (bool, error) {
	valType, err := GetTypeCategory(json)
	if err != nil {
		return false, err
	}
	return valType == jsonTypeObject, nil
}
