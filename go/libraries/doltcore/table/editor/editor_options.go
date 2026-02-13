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
	"github.com/dolthub/dolt/go/store/types"
)

const (
	invalidEaId = 0xFFFFFFFF
)

type PKDuplicateCb func(newKeyString, indexName string, existingKey, existingVal types.Tuple, isPk bool) error

// Options are properties that define different functionality for the tableEditSession.
// TODO next: all these fields are write-only, remove them
type Options struct {
	// TargetStaging is true if the table is being edited in the staging root, as opposed to the working root (rare).
	TargetStaging bool
}

func TestEditorOptions(vrw types.ValueReadWriter) Options {
	return Options{}
}
