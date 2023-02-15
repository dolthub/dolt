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

	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

// TraverseDAG traverses |old|, migrating values to |new|.
func TraverseDAG(ctx context.Context, menv Environment, old, new *doltdb.DoltDB) (err error) {
	var heads []ref.DoltRef
	var prog *progress

	heads, err = old.GetHeadRefs(ctx)
	if err != nil {
		return err
	}

	datasdb := doltdb.HackDatasDatabaseFromDoltDB(new)
	cs := datas.ChunkStoreFromDatabase(datasdb)

	prog, err = newProgress(ctx, cs)
	if err != nil {
		return err
	}

	for i := range heads {
		if err = traverseRefHistory(ctx, menv, heads[i], old, new, prog); err != nil {
			return err
		}
	}

	if err = validateBranchMapping(ctx, old, new); err != nil {
		return err
	}

	// write the migrated commit mapping to a special branch
	m, err := prog.Finalize(ctx)
	if err != nil {
		return err
	}
	if err = persistMigratedCommitMapping(ctx, new, m); err != nil {
		return err
	}
	return nil
}

func traverseRefHistory(ctx context.Context, menv Environment, r ref.DoltRef, old, new *doltdb.DoltDB, prog *progress) error {
	switch r.GetType() {
	case ref.BranchRefType:
		if err := traverseBranchHistory(ctx, menv, r, old, new, prog); err != nil {
			return err
		}
		wsRef, err := ref.WorkingSetRefForHead(r)
		if err != nil {
			return err
		}
		return migrateWorkingSet(ctx, menv, r.(ref.BranchRef), wsRef, old, new)

	case ref.TagRefType:
		return traverseTagHistory(ctx, menv, r.(ref.TagRef), old, new, prog)

	case ref.RemoteRefType:
		return traverseBranchHistory(ctx, menv, r, old, new, prog)

	case ref.WorkspaceRefType, ref.InternalRefType:
		return nil

	default:
		panic(fmt.Sprintf("unknown ref type %s", r.String()))
	}
}

func traverseBranchHistory(ctx context.Context, menv Environment, r ref.DoltRef, old, new *doltdb.DoltDB, prog *progress) error {
	cm, err := old.ResolveCommitRef(ctx, r)
	if err != nil {
		return err
	}
	if err = traverseCommitHistory(ctx, menv, cm, new, prog); err != nil {
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

func traverseTagHistory(ctx context.Context, menv Environment, r ref.TagRef, old, new *doltdb.DoltDB, prog *progress) error {
	t, err := old.ResolveTag(ctx, r)
	if err != nil {
		return err
	}

	if err = traverseCommitHistory(ctx, menv, t.Commit, new, prog); err != nil {
		return err
	}

	oldHash, err := t.Commit.HashOf()
	if err != nil {
		return err
	}
	newHash, err := prog.Get(ctx, oldHash)
	if err != nil {
		return err
	}
	cm, err := new.ReadCommit(ctx, newHash)
	if err != nil {
		return err
	}
	return new.NewTagAtCommit(ctx, r, cm, t.Meta)
}

func traverseCommitHistory(ctx context.Context, menv Environment, cm *doltdb.Commit, new *doltdb.DoltDB, prog *progress) error {
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
			if err = migrateCommit(ctx, menv, cm, new, prog); err != nil {
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

func firstAbsent(ctx context.Context, p *progress, addrs []hash.Hash) (int, error) {
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
