// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import "github.com/attic-labs/noms/go/types"

// CommitOptions is used to pass options into Commit.
type CommitOptions struct {
	// Parents, if provided is the parent commits of the commit we are creating.
	Parents types.Set
}
