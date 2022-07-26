// Copyright 2022 Dolthub, Inc.
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

package migrate

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

func validateMigration(ctx context.Context, old, new *doltdb.DoltDB) error {
	if err := validateBranchMapping(ctx, old, new); err != nil {
		return err
	}
	return nil
}

func validateBranchMapping(ctx context.Context, old, new *doltdb.DoltDB) error {
	branches, err := old.GetBranches(ctx)
	if err != nil {
		return err
	}

	var ok bool
	for _, bref := range branches {
		ok, err = new.HasBranch(ctx, bref.GetPath())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to map branch %s", bref.GetPath())
		}
	}
	return nil
}
