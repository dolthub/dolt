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

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

func TraverseDAG(ctx context.Context, old, new *doltdb.DoltDB) error {
	heads, err := old.GetHeadRefs(ctx)
	if err != nil {
		return err
	}

	prog := newProgress()
	for i := range heads {
		if err = TraverseRefHistory(ctx, heads[i], old, new, prog); err != nil {
			return err
		}
	}

	if err = ValidateMigration(ctx, old, new); err != nil {
		return err
	}
	return nil
}

func TraverseRefHistory(ctx context.Context, r ref.DoltRef, old, new *doltdb.DoltDB, prog Progress) error {
	switch r.GetType() {
	case ref.BranchRefType:
		if err := TraverseBranchHistory(ctx, r, old, new, prog); err != nil {
			return err
		}
		wsRef, err := ref.WorkingSetRefForHead(r)
		if err != nil {
			return err
		}
		return MigrateWorkingSet(ctx, wsRef, old, new, prog)

	case ref.TagRefType:
		return TraverseTagHistory(ctx, r.(ref.TagRef), old, new, prog)

	case ref.RemoteRefType:
		return TraverseBranchHistory(ctx, r, old, new, prog)

	case ref.WorkspaceRefType, ref.InternalRefType:
		return nil

	default:
		panic(fmt.Sprintf("unknown ref type %s", r.String()))
	}
}

func TraverseBranchHistory(ctx context.Context, r ref.DoltRef, old, new *doltdb.DoltDB, prog Progress) error {
	cm, err := old.ResolveCommitRef(ctx, r)
	if err != nil {
		return err
	}
	if err = TraverseCommitHistory(ctx, cm, new, prog); err != nil {
		return err
	}

	oldHash, err := cm.HashOf()
	if err != nil {
		return err
	}
	newHash, err := prog.Get(ctx, oldHash)
	if err != nil {
		return err
	}

	return new.SetHead(ctx, r, newHash)
}

func TraverseTagHistory(ctx context.Context, r ref.TagRef, old, new *doltdb.DoltDB, prog Progress) error {
	t, err := old.ResolveTag(ctx, r)
	if err != nil {
		return err
	}

	if err = TraverseCommitHistory(ctx, t.Commit, new, prog); err != nil {
		return err
	}

	return new.NewTagAtCommit(ctx, r, t.Commit, t.Meta)
}

func TraverseCommitHistory(ctx context.Context, cm *doltdb.Commit, new *doltdb.DoltDB, prog Progress) error {
	ch, err := cm.HashOf()
	if err != nil {
		return err
	}
	ok, err := prog.Has(ctx, ch)
	if err != nil || ok {
		return err
	}

	for {
		ph, err := cm.ParentHashes(ctx)
		if err != nil {
			return err
		}

		idx, err := firstAbsent(ctx, prog, ph)
		if err != nil {
			return err
		}
		if idx < 0 {
			// parents for |cm| are done, migrate |cm|
			if err = MigrateCommit(ctx, cm, new, prog); err != nil {
				return err
			}
			// pop the stack, traverse upwards
			cm, err = prog.Pop(ctx)
			if err != nil {
				return err
			}
			if cm == nil {
				return nil // done
			}
			continue
		}

		// push the stack, traverse downwards
		if err = prog.Push(ctx, cm); err != nil {
			return err
		}
		cm, err = cm.GetParent(ctx, idx)
		if err != nil {
			return err
		}
	}
}

func firstAbsent(ctx context.Context, p Progress, addrs []hash.Hash) (int, error) {
	for i := range addrs {
		ok, err := p.Has(ctx, addrs[i])
		if err != nil {
			return -1, err
		}
		if !ok {
			return i, nil
		}
	}
	return -1, nil
}
