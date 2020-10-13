// Copyright 2020 Liquidata, Inc.
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

package pipeline

import "context"

const localStorageKey = "ls"

// LocalStorage provides routine local storage on go routines spawned by a pipeline
type LocalStorage map[int]interface{}

// Get retrieves an item from localStorage
func (ls LocalStorage) Get(id int) (interface{}, bool) {
	val, ok := ls[id]
	return val, ok
}

// Put stores an item in local storage
func (ls LocalStorage) Put(id int, val interface{}) {
	ls[id] = val
}

// GetLocalStorage retrieves the LocalStorage from the context.  This only works if the context was generated
// by the pipeline package when starting the pipeline
func GetLocalStorage(ctx context.Context) LocalStorage {
	val := ctx.Value(localStorageKey)

	if val == nil {
		panic("This isn't the context for a pipeline spawned go routine, or the LocalStorage was deleted")
	}

	if ls, ok := val.(LocalStorage); !ok {
		panic("This isn't the context for a pipeline spawned go routine, or the LocalStorage was deleted")
	} else {
		return ls
	}
}
