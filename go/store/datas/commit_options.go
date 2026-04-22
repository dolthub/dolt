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

// CommitSigner signs a commit payload and returns the signature bytes.
// A nil CommitSigner in [CommitOptions] means the commit will not be signed.
type CommitSigner interface {
	Sign(ctx context.Context, payload []byte) ([]byte, error)
}

// CommitOptions is used to pass options into Commit.
type CommitOptions struct {
	// Meta contains the metadata for the commit.
	Meta *CommitMeta
	// Parents, if provided, is the parent commits of the commit we are
	// creating. If it is empty, the existing dataset head will be the only
	// parent.
	Parents []hash.Hash
	// Amend flag indicates that the commit being build it to amend an existing commit. Generally we add the branch HEAD
	// as a parent, in addition to the parent set provided here. When we amend, we want to strictly use the commits
	// provided in |Parents|, and no others.
	Amend bool
	// Signer, when non-nil, is called to sign the commit payload. DBName, HeadHash, and StagedHash
	// must also be set when Signer is provided.
	Signer CommitSigner
	// DBName is the name of the database, used when constructing the signature payload.
	DBName string
	// HeadHash is the hash of the current HEAD, used when constructing the signature payload.
	HeadHash hash.Hash
	// StagedHash is the hash of the staged root value, used when constructing the signature payload.
	StagedHash hash.Hash
}
