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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
)

// SignFunc is a function that signs a message and returns the signature.
type SignFunc func(ctx context.Context, keyId string, message []byte) ([]byte, error)

// SigningContext contains all information needed to sign a commit.
// If this is nil in CommitOptions, the commit will not be signed.
type SigningContext struct {
	// Key is the GPG key ID to use for signing.
	Key string
	// SignFunc is the function to use for signing.
	SignFunc SignFunc
	// DBName is the name of the database being committed to.
	DBName string
	// HeadHash is the hash of the current HEAD before the commit.
	HeadHash hash.Hash
	// StagedHash is the hash of the staged root value.
	StagedHash hash.Hash
}

// CommitOptions is used to pass options into Commit.
type CommitOptions struct {
	Meta *CommitMeta
	// Parents, if provided, is the parent commits of the commit we are
	// creating. If it is empty, the existing dataset head will be the only
	// parent.
	Parents []hash.Hash
	// Amend flag indicates that the commit being build it to amend an existing commit. Generally we add the branch HEAD
	// as a parent, in addition to the parent set provided here. When we amend, we want to strictly use the commits
	// provided in |Parents|, and no others.
	Amend bool
	// Signing contains all context needed for signing this commit. If nil, the commit will not be signed.
	Signing *SigningContext
}
