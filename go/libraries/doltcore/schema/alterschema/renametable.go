// Copyright 2019 Liquidata, Inc.
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

package alterschema

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// RenameTable renames a table with in a RootValue and returns the updated root.
func RenameTable(ctx context.Context, root *doltdb.RootValue, oldName, newName string) (*doltdb.RootValue, error) {
	if newName == oldName {
		return root, nil
	} else if root == nil {
		panic("invalid parameters")
	}

	return root.RenameTable(ctx, oldName, newName)
}
