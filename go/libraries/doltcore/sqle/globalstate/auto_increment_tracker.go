// Copyright 2021 Dolthub, Inc.
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

package globalstate

import (
	"fmt"
	"sync"
)

type AutoIncrementTracker interface {
	// Next returns the next auto increment value to be used by a table. If a table is not initialized in the counter
	// it will used the value stored in disk.
	Next(tableName string, insertVal interface{}, diskVal interface{}) (interface{}, error)
	// Reset resets the auto increment tracker value for a table. Typically used in truncate statements.
	Reset(tableName string, val interface{})
	// DropTable removes a table from the autoincrement tracker.
	DropTable(tableName string)
}

// AutoIncrementTracker is a global map that tracks which auto increment keys have been given for each table. At runtime
// it hands out the current key.
func NewAutoIncrementTracker() AutoIncrementTracker {
	return &autoIncrementTracker{
		valuePerTable: make(map[string]interface{}),
	}
}

type autoIncrementTracker struct {
	valuePerTable map[string]interface{}
	mu            sync.Mutex
}

var _ AutoIncrementTracker = (*autoIncrementTracker)(nil)

func (a *autoIncrementTracker) Next(tableName string, insertVal interface{}, diskVal interface{}) (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	diskVal = valOrZero(diskVal)

	// Case 0: Just use the value passed in.
	potential, ok := a.valuePerTable[tableName]
	if !ok {
		// Use the disk val if the table has not been initialized yet.
		potential = diskVal
	}

	// Case 1: Disk Val is greater. This is useful for updating the tracker when a merge occurs.
	// TODO: This is a bit of a hack. The correct solution is to plumb this tracker through the merge logic.
	diskValGreater, err := geq(valOrZero(diskVal), valOrZero(a.valuePerTable[tableName]))
	if err != nil {
		return nil, err
	}

	if diskValGreater {
		potential = diskVal
	}

	// Case 2: Overwrite anything if an insert val is passed.
	if insertVal != nil {
		potential = insertVal
	}

	// update the table only if val >= existing
	isGeq, err := geq(valOrZero(potential), valOrZero(a.valuePerTable[tableName]))
	if err != nil {
		return 0, err
	}

	if isGeq {
		val, err := convertIntTypeToUint(potential)
		if err != nil {
			return val, err
		}

		a.valuePerTable[tableName] = val + 1
	}

	return potential, nil
}

func (a *autoIncrementTracker) Reset(tableName string, val interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.valuePerTable[tableName] = val
}

func (a *autoIncrementTracker) DropTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.valuePerTable, tableName)
}

// Helper method that sets nil values to 0 for clarity purposes
func valOrZero(val interface{}) interface{} {
	if val == nil {
		return 0
	}

	return val
}

func geq(val1 interface{}, val2 interface{}) (bool, error) {
	v1, err := convertIntTypeToUint(val1)
	if err != nil {
		return false, err
	}

	v2, err := convertIntTypeToUint(val2)
	if err != nil {
		return false, err
	}

	return v1 >= v2, nil
}

func convertIntTypeToUint(val interface{}) (uint64, error) {
	switch t := val.(type) {
	case int:
		return uint64(t), nil
	case int8:
		return uint64(t), nil
	case int16:
		return uint64(t), nil
	case int32:
		return uint64(t), nil
	case int64:
		return uint64(t), nil
	case uint:
		return uint64(t), nil
	case uint8:
		return uint64(t), nil
	case uint16:
		return uint64(t), nil
	case uint32:
		return uint64(t), nil
	case uint64:
		return t, nil
	case float32:
		return uint64(t), nil
	case float64:
		return uint64(t), nil
	default:
		return 0, fmt.Errorf("error: auto increment is not a numeric type")
	}
}
