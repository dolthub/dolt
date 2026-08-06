// Copyright 2026 Dolthub, Inc.
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

package doltdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

func TestExistingRefErrorNamesTheRef(t *testing.T) {
	// See https://github.com/dolthub/dolt/issues/11434. A caller that does not
	// wrap this error shows it verbatim, so it has to say which ref was taken.
	// Two ref types may share a name, so the message carries the full path.
	for _, r := range []ref.DoltRef{
		ref.NewBranchRef("br"),
		ref.NewTagRef("br"),
		ref.NewWorkspaceRef("br"),
	} {
		err := &ExistingRefError{Ref: r}
		require.Equal(t, "ref '"+r.String()+"' already exists", err.Error())
	}
}
