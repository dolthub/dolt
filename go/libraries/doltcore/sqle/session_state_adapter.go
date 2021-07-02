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

package sqle

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

// SessionStateAdapter is an adapter for env.RepoStateReader in SQL contexts, getting information about the repo state
// from the session.
type SessionStateAdapter struct {
	session *dsess.Session
	dbName  string
}

var _ env.RepoStateReader = SessionStateAdapter{}
var _ env.RootsProvider = SessionStateAdapter{}

func NewSessionStateAdapter(session *dsess.Session, dbName string) SessionStateAdapter {
	return SessionStateAdapter{session: session, dbName: dbName}
}

func (s SessionStateAdapter) GetRoots(ctx context.Context) (doltdb.Roots, error) {
	return s.session.DbStates[s.dbName].GetRoots(), nil
}

func (s SessionStateAdapter) CWBHeadRef() ref.DoltRef {
	workingSet := s.session.DbStates[s.dbName].WorkingSet
	headRef, err := workingSet.Ref().ToHeadRef()
	// TODO: fix this interface
	if err != nil {
		panic(err)
	}
	return headRef
}

func (s SessionStateAdapter) CWBHeadSpec() *doltdb.CommitSpec {
	// TODO: get rid of this
	ref := s.CWBHeadRef()
	spec, err := doltdb.NewCommitSpec(ref.GetPath())
	if err != nil {
		panic(err)
	}
	return spec
}

func (s SessionStateAdapter) IsMergeActive(ctx context.Context) (bool, error) {
	panic("implement me")
}

func (s SessionStateAdapter) GetMergeCommit(ctx context.Context) (*doltdb.Commit, error) {
	panic("implement me")
}

func (s SessionStateAdapter) GetPreMergeWorking(ctx context.Context) (*doltdb.RootValue, error) {
	panic("implement me")
}

func (s SessionStateAdapter) GetRemotes() ([]env.Remote) {
	panic("implement me")
}
