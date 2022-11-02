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

package merge

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/store/types"
)

type conflictStash struct {
	Stash map[string]*conflictData
}

type conflictData struct {
	HasConflicts bool
	Sch          conflict.ConflictSchema
	ConfIdx      durable.ConflictIndex
}

// Empty returns false if any table has a conflict.
// True otherwise.
func (s *conflictStash) Empty() bool {
	for _, data := range s.Stash {
		if data.HasConflicts {
			return false
		}
	}
	return true
}

type violationStash struct {
	// todo: durable
	Stash map[string]types.Map
}

// Empty returns false if any table has constraint violations.
// True otherwise.
func (s *violationStash) Empty() bool {
	for _, data := range s.Stash {
		if data.Len() > 0 {
			return false
		}
	}
	return true
}

func stashConflicts(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, *conflictStash, error) {
	names, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, nil, err
	}

	updatedRoot := root
	stash := make(map[string]*conflictData, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, nil, err
		}
		d, err := getConflictData(ctx, tbl)
		if err != nil {
			return nil, nil, err
		}
		stash[name] = d
		tbl, err = tbl.ClearConflicts(ctx)
		if err != nil {
			return nil, nil, err
		}
		updatedRoot, err = updatedRoot.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, nil, err
		}
	}

	return updatedRoot, &conflictStash{stash}, nil
}

func stashViolations(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, *violationStash, error) {
	names, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, nil, err
	}

	updatedRoot := root
	stash := make(map[string]types.Map, len(names))
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, nil, err
		}
		v, err := tbl.GetConstraintViolations(ctx)
		stash[name] = v
		tbl, err = tbl.SetConstraintViolations(ctx, types.EmptyMap)
		if err != nil {
			return nil, nil, err
		}
		updatedRoot, err = updatedRoot.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, nil, err
		}
	}

	return updatedRoot, &violationStash{stash}, nil
}

// applyConflictStash applies the data in |stash| to the root value. Missing
// tables will be skipped. This function will override any previous conflict
// data.
func applyConflictStash(ctx context.Context, stash map[string]*conflictData, root *doltdb.RootValue) (*doltdb.RootValue, error) {
	updatedRoot := root
	for name, data := range stash {
		tbl, ok, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		tbl, err = setConflictData(ctx, tbl, data)
		if err != nil {
			return nil, err
		}
		updatedRoot, err = updatedRoot.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}

	return updatedRoot, nil
}

func getConflictData(ctx context.Context, tbl *doltdb.Table) (*conflictData, error) {
	var sch conflict.ConflictSchema
	var confIdx durable.ConflictIndex

	hasCnf, err := tbl.HasConflicts(ctx)
	if err != nil {
		return nil, err
	}
	if hasCnf {
		sch, confIdx, err = tbl.GetConflicts(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &conflictData{
		HasConflicts: hasCnf,
		Sch:          sch,
		ConfIdx:      confIdx,
	}, nil
}

func setConflictData(ctx context.Context, tbl *doltdb.Table, data *conflictData) (*doltdb.Table, error) {
	var err error
	if !data.HasConflicts {
		tbl, err = tbl.ClearConflicts(ctx)
	} else {
		tbl, err = tbl.SetConflicts(ctx, data.Sch, data.ConfIdx)
	}
	if err != nil {
		return nil, err
	}

	return tbl, nil
}
