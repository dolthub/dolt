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

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

func TraverseDAG(ctx context.Context, ddb *doltdb.DoltDB) (Progress, error) {
	heads, err := ddb.GetHeadRefs(ctx)
	if err != nil {
		return nil, err
	}

	prog := newProgress()
	for i := range heads {
		if err = TraverseHistory(ctx, heads[i], prog); err != nil {
			return nil, err
		}
	}
	return prog, nil
}

func TraverseHistory(ctx context.Context, r ref.DoltRef, prog Progress) error {
	return nil
}

func TraverseTagHistory(ctx context.Context, tag *doltdb.Tag, prog Progress) error {
	return nil
}

func TraverseWorkingSetHistory(ctx context.Context, ws doltdb.WorkingSet, prog Progress) error {
	return nil
}

func TraverseCommitHistory(ctx context.Context, cm *doltdb.Commit, prog Progress) error {
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
			if err = MigrateCommit(ctx, cm, prog); err != nil {
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
