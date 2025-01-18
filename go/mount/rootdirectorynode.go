// Copyright 2025 Dolthub, Inc.
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

package mount

import (
	"bazil.org/fuse/fs"
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"syscall"
)

type rootDirectoryNode struct {
	BaseDirectory
	dEnv *env.DoltEnv
	db   *doltdb.DoltDB
}

func (d rootDirectoryNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "head", "HEAD":
		headRoot, err := d.dEnv.HeadRoot(ctx)
		if err != nil {
			return nil, err
		}
		return rootValueDirectoryNode{rootValue: headRoot}, nil
	case "working", "WORKING":
		roots, err := d.dEnv.Roots(ctx)
		if err != nil {
			return nil, err
		}
		return rootValueDirectoryNode{rootValue: roots.Working}, nil
	case "staged", "STAGED":
		roots, err := d.dEnv.Roots(ctx)
		if err != nil {
			return nil, err
		}
		return rootValueDirectoryNode{rootValue: roots.Staged}, nil
	}

	// is it a branch?
	{
		branches, err := d.dEnv.GetBranches()
		if err != nil {
			return nil, err
		}

		branch, hasBranch := branches.Get(name)
		if hasBranch {
			path := branch.Merge.Ref.GetPath()
			refHash, err := d.db.GetHashForRefStr(ctx, path)
			if err != nil {
				return nil, err
			}
			return lookupHash(ctx, d.db, *refHash)
		}
	}

	// is it a tag?
	{
		tags, err := d.db.GetTagsWithHashes(ctx)
		if err != nil {
			return nil, err
		}

		for _, tag := range tags {
			if tag.Tag.Name == name {
				return commitDirectory{commit: tag.Tag.Commit}, nil
			}
		}
	}

	// is it a remote?
	{
		// or use GetRemotesWithHashes?
		remotes, err := d.dEnv.GetRemotes()
		if err != nil {
			return nil, err
		}
		_, hasRemote := remotes.Get(name)
		if hasRemote {
			// TODO: How to resolve a remote to a list of refs?
		}
	}

	// is it an address?

	node, err := addressesDirectory{db: d.db}.Lookup(ctx, name)
	if err != syscall.ENOENT {
		return node, err
	}

	// when do we check when a ref spec is complete? Now when we add to it? Or when we do a lookup on it?
	// probably noe.
	return lookupRefSpec(ctx, d.db, name)
}
