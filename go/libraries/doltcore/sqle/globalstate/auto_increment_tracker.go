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
	"context"
	"math"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// NewAutoIncrementTracker returns a new autoincrement tracker for the working sets given
func NewAutoIncrementTracker(ctx context.Context, wses ...*doltdb.WorkingSet) (AutoIncrementTracker, error) {
	ait := AutoIncrementTracker{
		sequences: make(map[string]uint64),
		mu:        &sync.Mutex{},
	}

	// collect auto increment values from all working sets given
	for _, ws := range wses {
		err := ws.WorkingRoot().IterTables(ctx, func(tableName string, table *doltdb.Table, sch schema.Schema) (bool, error) {
			ok := schema.HasAutoIncrement(sch)
			if !ok {
				return false, nil
			}

			tableName = strings.ToLower(tableName)

			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return true, err
			}

			if seq > ait.sequences[tableName] {
				ait.sequences[tableName] = seq
			}

			return false, nil
		})

		if err != nil {
			return AutoIncrementTracker{}, err
		}
	}

	return ait, nil
}

type AutoIncrementTracker struct {
	sequences map[string]uint64
	mu        *sync.Mutex
}

func (a AutoIncrementTracker) Current(tableName string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sequences[strings.ToLower(tableName)]
}

func (a AutoIncrementTracker) Next(tbl string, insertVal interface{}) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tbl = strings.ToLower(tbl)

	given, err := CoerceAutoIncrementValue(insertVal)
	if err != nil {
		return 0, err
	}

	curr := a.sequences[tbl]

	if given == 0 {
		// |given| is 0 or NULL
		a.sequences[tbl]++
		return curr, nil
	}

	if given >= curr {
		a.sequences[tbl] = given
		a.sequences[tbl]++
		return given, nil
	}

	// |given| < curr
	return given, nil
}


// CoerceAutoIncrementValue converts |val| into an AUTO_INCREMENT sequence value
func CoerceAutoIncrementValue(val interface{}) (uint64, error) {
	switch typ := val.(type) {
	case float32:
		val = math.Round(float64(typ))
	case float64:
		val = math.Round(typ)
	}

	var err error
	val, err = sql.Uint64.Convert(val)
	if err != nil {
		return 0, err
	}
	if val == nil || val == uint64(0) {
		return 0, nil
	}
	return val.(uint64), nil
}

// TODO: this needs to change
func (a AutoIncrementTracker) Set(tableName string, val uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sequences[strings.ToLower(tableName)] = val
}

func (a AutoIncrementTracker) AddNewTable(tableName string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sequences[strings.ToLower(tableName)] = uint64(1)
}

func (a AutoIncrementTracker) DropTable(ctx context.Context, tableName string, wses ...*doltdb.WorkingSet) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	tableName = strings.ToLower(tableName)
	delete(a.sequences, tableName)

	// Get the new highest value from all tables in the working sets given
	for _, ws := range wses {
		table, _, exists, err := ws.WorkingRoot().GetTableInsensitive(ctx, tableName)
		if err != nil {
			return err
		}

		if !exists {
			continue
		}

		sch, err := table.GetSchema(ctx)
		if err != nil {
			return err
		}

		if schema.HasAutoIncrement(sch) {
			seq, err := table.GetAutoIncrementValue(ctx)
			if err != nil {
				return err
			}

			if seq > a.sequences[tableName] {
				a.sequences[tableName] = seq
			}
		}
	}

	return nil
}
