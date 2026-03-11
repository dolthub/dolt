// Copyright 2019 Dolthub, Inc.
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

package row

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/types"
)

// TaggedValues is a map of tag to value, used in the original implementation of row storage with noms types.
// Use |val.Tuple| instead.
// Deprecated
type TaggedValues map[uint64]types.Value

func (tt TaggedValues) Iter(cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	stop := false

	var err error
	for tag, val := range tt {
		stop, err = cb(tag, val)

		if stop || err != nil {
			break
		}
	}

	return stop, err
}

func (tt TaggedValues) Get(tag uint64) (types.Value, bool) {
	val, ok := tt[tag]
	return val, ok
}

func (tt TaggedValues) String() string {
	str := "{"
	for k, v := range tt {
		encStr, err := types.EncodedValue(context.Background(), v)

		if err != nil {
			return err.Error()
		}

		str += fmt.Sprintf("\n\t%d: %s", k, encStr)
	}

	str += "\n}"
	return str
}
