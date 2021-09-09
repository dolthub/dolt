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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

func NewMemoryDbData(ctx context.Context) (DbData, error) {
	ddb, err := NewMemoryDoltDB(ctx)
	if err != nil {
		return DbData{}, err
	}

	rs, err := NewMemoryRepoState(ctx)
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

func NewMemoryDoltDB(ctx context.Context) (*doltdb.DoltDB, error) {
	return nil, nil
}

func NewMemoryRepoState(ctx context.Context) (MemoryRepoState, error) {
	return MemoryRepoState{}, nil
}

type MemoryRepoState struct {
	byte
}

var _ RepoStateReader = MemoryRepoState{}
var _ RepoStateWriter = MemoryRepoState{}
var _ DocsReadWriter = MemoryRepoState{}

func (m MemoryRepoState) CWBHeadRef() ref.DoltRef {
	panic("unimplemented")
}

func (m MemoryRepoState) CWBHeadSpec() *doltdb.CommitSpec {
	panic("unimplemented")
}

func (m MemoryRepoState) GetRemotes() (map[string]Remote, error) {
	panic("unimplemented")
}

func (m MemoryRepoState) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	panic("unimplemented")
}

func (m MemoryRepoState) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	panic("unimplemented")
}

func (m MemoryRepoState) SetCWBHeadRef(context.Context, ref.MarshalableRef) error {
	panic("unimplemented")
}

func (m MemoryRepoState) AddRemote(name string, url string, fetchSpecs []string, params map[string]string) error {
	panic("unimplemented")
}

func (m MemoryRepoState) RemoveRemote(ctx context.Context, name string) error {
	panic("unimplemented")
}

func (m MemoryRepoState) GetDocsOnDisk(docNames ...string) (doltdocs.Docs, error) {
	panic("unimplemented")
}

func (m MemoryRepoState) WriteDocsToDisk(docs doltdocs.Docs) error {
	panic("unimplemented")
}
