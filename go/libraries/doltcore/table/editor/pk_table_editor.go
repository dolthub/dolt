// Copyright 2020 Dolthub, Inc.
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

package editor

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/store/types"
)

const tfApproxCapacity = 64

var tupleFactories = &sync.Pool{New: func() interface{} {
	return types.NewTupleFactory(tfApproxCapacity)
}}

type PKDuplicateCb func(newKeyString, indexName string, existingKey, existingVal types.Tuple, isPk bool) error

// Options are properties that define different functionality for the tableEditSession.
type Options struct {
	ForeignKeyChecksDisabled bool // If true, then ALL foreign key checks AND updates (through CASCADE, etc.) are skipped
	Deaf                     DbEaFactory
	Tempdir                  string

	// TargetStaging is true if the table is being edited in the staging root, as opposed to the working root (rare).
	TargetStaging bool
}

// WithDeaf returns a new Options with the given  edit accumulator factory class
func (o Options) WithDeaf(deaf DbEaFactory) Options {
	o.Deaf = deaf
	return o
}

func TestEditorOptions(vrw types.ValueReadWriter) Options {
	return Options{
		ForeignKeyChecksDisabled: false,
		Deaf:                     NewInMemDeaf(vrw),
	}
}

// formatKey returns a comma-separated string representation of the key given.
func formatKey(ctx context.Context, key types.Value) (string, error) {
	tuple, ok := key.(types.Tuple)
	if !ok {
		return "", fmt.Errorf("Expected types.Tuple but got %T", key)
	}

	var vals []string
	iter, err := tuple.Iterator()
	if err != nil {
		return "", err
	}

	for iter.HasMore() {
		i, val, err := iter.Next()
		if err != nil {
			return "", err
		}
		if i%2 == 1 {
			str, err := types.EncodedValue(ctx, val)
			if err != nil {
				return "", err
			}
			vals = append(vals, str)
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(vals, ",")), nil
}
