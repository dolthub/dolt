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
	"context"
	"maps"

	"github.com/dolthub/dolt/go/store/hash"
)

// RootsStatus holds a [Roots] bundle's staged and unstaged table diffs. Construct with
// [NewRootsStatus]; a nil value means the branch has no uncommitted changes.
type RootsStatus struct {
	// Head holds the committed table hashes, used to classify entries in Staged and Unstaged.
	Head map[TableName]hash.Hash
	// Staged is the head-to-staged diff: tables where staged differs from head.
	Staged map[TableName]hash.Hash
	// Unstaged is the staged-to-working diff: tables where working differs from staged.
	Unstaged map[TableName]hash.Hash
}

// NewRootsStatus returns |roots|' staged and unstaged table diffs, or nil when both are
// empty. Read-only system tables and tables in |ignored| are excluded.
func NewRootsStatus(ctx context.Context, roots Roots, ignored *TableNameSet) (*RootsStatus, error) {
	head, err := userTableHashes(ctx, roots.Head, ignored)
	if err != nil {
		return nil, err
	}
	stagedHashes, err := userTableHashes(ctx, roots.Staged, ignored)
	if err != nil {
		return nil, err
	}
	workingHashes, err := userTableHashes(ctx, roots.Working, ignored)
	if err != nil {
		return nil, err
	}
	staged := DiffTableHashes(head, stagedHashes)
	unstaged := DiffTableHashes(stagedHashes, workingHashes)
	if len(staged) == 0 && len(unstaged) == 0 {
		return nil, nil
	}
	return &RootsStatus{Head: head, Staged: staged, Unstaged: unstaged}, nil
}

// Added returns staged tables that are absent from head.
func (s *RootsStatus) Added() []TableName {
	var out []TableName
	for name := range s.Staged {
		if _, inHead := s.Head[name]; !inHead {
			out = append(out, name)
		}
	}
	return out
}

// Untracked returns unstaged tables that are absent from both staged and head.
func (s *RootsStatus) Untracked() []TableName {
	var out []TableName
	for name := range s.Unstaged {
		if _, inHead := s.Head[name]; inHead {
			continue
		}
		if _, inStaged := s.Staged[name]; inStaged {
			continue
		}
		out = append(out, name)
	}
	return out
}

// userTableHashes returns |root|'s table hashes excluding read-only system tables and any
// table named in |ignored|.
func userTableHashes(ctx context.Context, root RootValue, ignored *TableNameSet) (map[TableName]hash.Hash, error) {
	hashes, err := MapTableHashes(ctx, root)
	if err != nil {
		return nil, err
	}
	maps.DeleteFunc(hashes, func(name TableName, _ hash.Hash) bool {
		return IsReadOnlySystemTable(name) || ignored.Contains(name)
	})
	return hashes, nil
}
