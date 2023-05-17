// Copyright 2023 Dolthub, Inc.
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

package sqle

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

func init() {
	// The transaction logic in the dsess package needs to use the merge code when committing, to merge
	// staged changes into the current HEAD when creating a new commit. Since the merge package also
	// needs the dsess package, we break the package import cycle by setting this interface in dsess
	// with a default implementation here that uses the merge package.
	dsess.DefaultTransactionRootMerger = &defaultTransactionRootMerger{}
}

type defaultTransactionRootMerger struct{}

var _ dsess.TransactionRootMerger = defaultTransactionRootMerger{}

// MergeRoots implements the dsess.TransactionRootMerger interface.
func (d defaultTransactionRootMerger) MergeRoots(ctx context.Context,
	ourRoot, theirRoot, ancRoot *doltdb.RootValue,
	theirs, ancestor doltdb.Rootish,
	opts editor.Options) (*doltdb.RootValue, error) {

	result, err := merge.MergeRoots(ctx, ourRoot, theirRoot, ancRoot, theirs, ancestor, opts, merge.MergeOpts{})
	if err != nil {
		return nil, err
	}

	return result.Root, nil
}
