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

package dsess

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
)

// SessionStateAdapter is an adapter for env.RepoStateReader in SQL contexts, getting information about the repo state
// from the session.
type SessionStateAdapter struct {
	session  *DoltSession
	remotes  *concurrentmap.Map[string, env.Remote]
	backups  *concurrentmap.Map[string, env.Remote]
	branches *concurrentmap.Map[string, env.BranchConfig]
	dbName   string
}

func (s SessionStateAdapter) SetCWBHeadRef(ctx context.Context, newRef ref.MarshalableRef) error {
	return fmt.Errorf("Cannot set cwb head ref with a SessionStateAdapter")
}

var _ env.RepoStateReader[*sql.Context] = SessionStateAdapter{}
var _ env.RepoStateWriter = SessionStateAdapter{}
var _ env.RootsProvider[*sql.Context] = SessionStateAdapter{}

func NewSessionStateAdapter(session *DoltSession, dbName string, remotes *concurrentmap.Map[string, env.Remote], branches *concurrentmap.Map[string, env.BranchConfig], backups *concurrentmap.Map[string, env.Remote]) SessionStateAdapter {
	if branches == nil {
		branches = concurrentmap.New[string, env.BranchConfig]()
	}
	return SessionStateAdapter{session: session, dbName: dbName, remotes: remotes, branches: branches, backups: backups}
}

func (s SessionStateAdapter) GetRoots(ctx *sql.Context) (doltdb.Roots, error) {
	state, _, err := s.session.lookupDbState(ctx, s.dbName)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return state.roots(), nil
}

func (s SessionStateAdapter) CWBHeadRef(ctx *sql.Context) (ref.DoltRef, error) {
	workingSet, err := s.session.WorkingSet(ctx, s.dbName)
	if err != nil {
		return nil, err
	}

	headRef, err := workingSet.Ref().ToHeadRef()
	if err != nil {
		return nil, err
	}
	return headRef, nil
}

func (s SessionStateAdapter) CWBHeadSpec(ctx *sql.Context) (*doltdb.CommitSpec, error) {
	// TODO: get rid of this
	ref, err := s.CWBHeadRef(ctx)
	if err != nil {
		return nil, err
	}
	spec, err := doltdb.NewCommitSpec(ref.GetPath())
	if err != nil {
		panic(err)
	}
	return spec, nil
}

func (s SessionStateAdapter) GetRemotes() (*concurrentmap.Map[string, env.Remote], error) {
	return s.remotes, nil
}

func (s SessionStateAdapter) GetBackups() (*concurrentmap.Map[string, env.Remote], error) {
	return s.backups, nil
}

func (s SessionStateAdapter) GetBranches() (*concurrentmap.Map[string, env.BranchConfig], error) {
	return s.branches, nil
}

func (s SessionStateAdapter) UpdateBranch(name string, new env.BranchConfig) error {
	s.branches.Set(name, new)

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}
	repoState.Branches.Set(name, new)

	return repoState.Save(fs)
}

func (s SessionStateAdapter) AddRemote(remote env.Remote) error {
	if _, ok := s.remotes.Get(remote.Name); ok {
		return env.ErrRemoteAlreadyExists
	}

	if strings.IndexAny(remote.Name, env.InvalidRemoteNameCharacters) != -1 {
		return env.ErrInvalidRemoteName
	}

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	// can have multiple remotes with the same address, but no conflicting backups
	if rem, found := env.CheckRemoteAddressConflict(remote.Url, nil, repoState.Backups); found {
		return env.ErrRemoteAddressConflict.New(rem.Name, rem.Url)
	}

	s.remotes.Set(remote.Name, remote)
	repoState.AddRemote(remote)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) AddBackup(remote env.Remote) error {
	if remote.Name == "" || strings.IndexAny(remote.Name, env.InvalidRemoteNameCharacters) != -1 {
		return env.ErrBackupInvalidName.New(remote.Name)
	}

	if _, ok := s.backups.Get(remote.Name); ok {
		return env.ErrBackupAlreadyExists.New(remote.Name)
	}

	if conflict, found := env.CheckRemoteAddressConflict(remote.Url, s.remotes, s.backups); found {
		return env.ErrRemoteAddressConflict.New(conflict.Name, conflict.Url)
	}

	s.backups.Set(remote.Name, remote)

	fileSys, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}
	parsedRepoState, err := env.LoadRepoState(fileSys)
	if err != nil {
		return err
	}

	// TODO(elianddb): This is a known limitation of repo_state.json; may lose concurrent modifications.
	//  See: https://www.dolthub.com/blog/2021-08-06-long-dark-rewrite-of-the-soul/
	parsedRepoState.Backups = s.backups
	return parsedRepoState.Save(fileSys)
}

func (s SessionStateAdapter) RemoveRemote(_ context.Context, name string) error {
	remote, ok := s.remotes.Get(name)
	if !ok {
		return env.ErrRemoteNotFound
	}
	s.remotes.Delete(remote.Name)

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	remote, ok = repoState.Remotes.Get(name)
	if !ok {
		// sanity check
		return env.ErrRemoteNotFound
	}
	repoState.Remotes.Delete(name)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) RemoveBackup(_ context.Context, name string) error {
	_, ok := s.backups.Get(name)
	if !ok {
		return env.ErrBackupNotFound.New(name)
	}
	s.backups.Delete(name)

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	parsedRepoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	// TODO(elianddb): This is a known limitation of repo_state.json; may lose concurrent modifications.
	//  See: https://www.dolthub.com/blog/2021-08-06-long-dark-rewrite-of-the-soul/
	parsedRepoState.Backups = s.backups
	return parsedRepoState.Save(fs)
}

func (s SessionStateAdapter) TempTableFilesDir() (string, error) {
	branchState, _, err := s.session.lookupDbState(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		return "", err
	}

	return branchState.dbState.tmpFileDir, nil
}
