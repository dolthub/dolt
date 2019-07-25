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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"github.com/liquidata-inc/dolt/go/store/merge"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// CommitOptions is used to pass options into Commit.
type CommitOptions struct {
	// Parents, if provided is the parent commits of the commit we are
	// creating.
	Parents types.Set

	// Meta is a Struct that describes arbitrary metadata about this Commit,
	// e.g. a timestamp or descriptive text.
	Meta types.Struct

	// Policy will be called to attempt to merge this Commit with the current
	// Head, if this is not a fast-forward. If Policy is nil, no merging will
	// be attempted. Note that because Commit() retries in some cases, Policy
	// might also be called multiple times with different values.
	Policy merge.Policy
}
