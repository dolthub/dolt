// Copyright 2021 Dolthub, Inc.
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

package env

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

func NewMemoryDbData(ctx context.Context, cfg config.ReadableConfig) (DbData, error) {
	branchName := GetDefaultInitBranch(cfg)

	ddb, err := NewMemoryDoltDB(ctx, branchName)
	if err != nil {
		return DbData{}, err
	}

	rs, err := NewMemoryRepoState(ctx, ddb, branchName)
	if err != nil {
		return DbData{}, err
	}

	return DbData{
		Ddb: ddb,
		Rsw: rs,
		Rsr: rs,
		Drw: rs,
	}, nil
}

func NewMemoryDoltDB(ctx context.Context, initBranch string) (*doltdb.DoltDB, error) {
	ts := &chunks.TestStorage{}
	cs := ts.NewViewWithDefaultFormat()
	ddb := doltdb.DoltDBFromCS(cs)

	m := "memory"
	branchRef := ref.NewBranchRef(initBranch)
	err := ddb.WriteEmptyRepoWithCommitTimeAndDefaultBranch(ctx, m, m, datas.CommitNowFunc(), branchRef)
	if err != nil {
		return nil, err
	}

	return ddb, nil
}

func NewMemoryRepoState(ctx context.Context, ddb *doltdb.DoltDB, initBranch string) (MemoryRepoState, error) {
	head := ref.NewBranchRef(initBranch)
	rs := MemoryRepoState{
		DoltDB: ddb,
		Head:   head,
	}

	commit, err := ddb.ResolveCommitRef(ctx, head)
	if err != nil {
		return MemoryRepoState{}, err
	}

	root, err := commit.GetRootValue(ctx)
	if err != nil {
		return MemoryRepoState{}, err
	}

	err = rs.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return MemoryRepoState{}, err
	}

	err = rs.UpdateStagedRoot(ctx, root)
	if err != nil {
		return MemoryRepoState{}, err
	}

	return rs, nil
}

type MemoryRepoState struct {
	DoltDB *doltdb.DoltDB
	Head   ref.DoltRef
}

var _ RepoStateReader = MemoryRepoState{}
var _ RepoStateWriter = MemoryRepoState{}
var _ DocsReadWriter = MemoryRepoState{}

func (m MemoryRepoState) CWBHeadRef() ref.DoltRef {
	return m.Head
}

func (m MemoryRepoState) CWBHeadSpec() *doltdb.CommitSpec {
	spec, err := doltdb.NewCommitSpec(m.CWBHeadRef().GetPath())
	if err != nil {
		panic(err)
	}
	return spec
}

func (m MemoryRepoState) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := m.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(m.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(newRoot).WithStagedRoot(newRoot)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	return m.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithStagedRoot(newRoot), h, m.workingSetMeta())
}

func (m MemoryRepoState) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := m.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(m.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(newRoot).WithStagedRoot(newRoot)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	return m.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithWorkingRoot(newRoot), h, m.workingSetMeta())
}

func (m MemoryRepoState) WorkingSet(ctx context.Context) (*doltdb.WorkingSet, error) {
	workingSetRef, err := ref.WorkingSetRefForHead(m.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	workingSet, err := m.DoltDB.ResolveWorkingSet(ctx, workingSetRef)
	if err != nil {
		return nil, err
	}

	return workingSet, nil
}

func (m MemoryRepoState) workingSetMeta() *datas.WorkingSetMeta {
	return &datas.WorkingSetMeta{
		Timestamp:   uint64(time.Now().Unix()),
		Description: "updated from dolt environment",
	}
}

func (m MemoryRepoState) SetCWBHeadRef(_ context.Context, r ref.MarshalableRef) (err error) {
	m.Head = r.Ref
	return
}

func (m MemoryRepoState) GetRemotes() (map[string]Remote, error) {
	return make(map[string]Remote), nil
}

func (m MemoryRepoState) AddRemote(name string, url string, fetchSpecs []string, params map[string]string) error {
	return fmt.Errorf("cannot insert a remote in a memory database")
}

func (m MemoryRepoState) GetBranches() (map[string]BranchConfig, error) {
	return make(map[string]BranchConfig), nil
}

func (m MemoryRepoState) UpdateBranch(name string, new BranchConfig) error {
	return nil
}

func (m MemoryRepoState) RemoveRemote(ctx context.Context, name string) error {
	return fmt.Errorf("cannot delete a remote from a memory database")
}

func (m MemoryRepoState) TempTableFilesDir() string {
	return os.TempDir()
}

func (m MemoryRepoState) GetDocsOnDisk(docNames ...string) (doltdocs.Docs, error) {
	return nil, fmt.Errorf("cannot get docs from a memory database")
}

func (m MemoryRepoState) WriteDocsToDisk(docs doltdocs.Docs) error {
	return fmt.Errorf("cannot write docs to a memory database")
}

func (m MemoryRepoState) GetBackups() (map[string]Remote, error) {
	panic("cannot get backups on in memory database")
}

func (m MemoryRepoState) AddBackup(name string, url string, fetchSpecs []string, params map[string]string) error {
	panic("cannot add backup to in memory database")
}

func (m MemoryRepoState) RemoveBackup(ctx context.Context, name string) error {
	panic("cannot remove backup from in memory database")
}
