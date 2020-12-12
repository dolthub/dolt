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

package row

import (
	"fmt"

	"github.com/dolthub/dolt/go/store/types"
)

func iterPkTuple(tvs types.TupleValueSlice, cb func(tag uint64, val types.Value) (stop bool, err error)) error {
	if len(tvs)%2 != 0 {
		return fmt.Errorf("expected len(TupleValueSlice) to be even, got %d", len(tvs))
	}
	return iterTupleFrom(tvs, 0, cb)
}

func iterKeylessTuple(tvs types.TupleValueSlice, cb func(tag uint64, val types.Value) (stop bool, err error)) error {
	if len(tvs)%2 != 1 {
		return fmt.Errorf("expected len(TupleValueSlice) to be odd, got %d", len(tvs))
	}
	return iterTupleFrom(tvs, 1, cb)
}

func iterTupleFrom(tvs types.TupleValueSlice, start int, cb func(tag uint64, val types.Value) (stop bool, err error)) error {
	l := len(tvs)
	for i := start; i < l; i += 2 {
		stop, err := cb(uint64(tvs[i].(types.Uint)), tvs[i+1])

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}
