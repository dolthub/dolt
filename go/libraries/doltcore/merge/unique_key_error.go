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

package merge

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/store/types"
)

// uniqueKeyError is a unique key error as encountered by a table editor.
type uniqueKeyError struct {
	indexName string
	k         types.Tuple
	v         types.Tuple
}

var _ error = uniqueKeyError{}

// Error implements the interface error.
func (u uniqueKeyError) Error() string {
	var vals []string
	for _, tpl := range []types.Tuple{u.k, u.v} {
		iter, err := tpl.Iterator()
		if err != nil {
			return err.Error()
		}
		for iter.HasMore() {
			i, val, err := iter.Next()
			if err != nil {
				return err.Error()
			}
			if i%2 == 1 {
				str, err := types.EncodedValue(context.Background(), val)
				if err != nil {
					return err.Error()
				}
				vals = append(vals, str)
			}
		}
	}
	return fmt.Sprintf("duplicate unique key: [%s]", strings.Join(vals, ","))
}

// handleTableEditorDuplicateErr handles duplicate keys within a table editor for merge operations. This is intended
// to handle unique key errors for the purpose of constraint violations.
func handleTableEditorDuplicateErr(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	if isPk {
		return fmt.Errorf("duplicate key '%s'", keyString)
	}
	return uniqueKeyError{
		indexName: indexName,
		k:         k,
		v:         v,
	}
}
