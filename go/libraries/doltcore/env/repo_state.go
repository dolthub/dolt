// Copyright 2019 Dolthub, Inc.
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
	"encoding/json"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type RepoStateReader interface {
	CWBHeadRef() ref.DoltRef
	CWBHeadSpec() *doltdb.CommitSpec
	GetRemotes() (map[string]Remote, error)
	GetBackups() (map[string]Remote, error)
	GetBranches() (map[string]BranchConfig, error)
}

type RepoStateWriter interface {
	// TODO: get rid of this
	UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error
	// TODO: get rid of this
	UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error
	SetCWBHeadRef(context.Context, ref.MarshalableRef) error
	AddRemote(r Remote) error
	AddBackup(r Remote) error
	RemoveRemote(ctx context.Context, name string) error
	RemoveBackup(ctx context.Context, name string) error
	TempTableFilesDir() (string, error)
	UpdateBranch(name string, new BranchConfig) error
}

type DbData struct {
	Ddb *doltdb.DoltDB
	Rsw RepoStateWriter
	Rsr RepoStateReader
}

type BranchConfig struct {
	Merge  ref.MarshalableRef `json:"head"`
	Remote string             `json:"remote"`
}

type RepoState struct {
	Head     ref.MarshalableRef      `json:"head"`
	Remotes  map[string]Remote       `json:"remotes"`
	Backups  map[string]Remote       `json:"backups"`
	Branches map[string]BranchConfig `json:"branches"`
	// |staged|, |working|, and |merge| are legacy fields left over from when Dolt repos stored this info in the repo
	// state file, not in the DB directly. They're still here so that we can migrate existing repositories forward to the
	// new storage format, but they should be used only for this purpose and are no longer written.
	staged  string
	working string
	merge   *mergeState
}

// repoStateLegacy only exists to unmarshall legacy repo state files, since the JSON marshaller can't work with
// unexported fields
type repoStateLegacy struct {
	Head     ref.MarshalableRef      `json:"head"`
	Remotes  map[string]Remote       `json:"remotes"`
	Backups  map[string]Remote       `json:"backups"`
	Branches map[string]BranchConfig `json:"branches"`
	Staged   string                  `json:"staged,omitempty"`
	Working  string                  `json:"working,omitempty"`
	Merge    *mergeState             `json:"merge,omitempty"`
}

// repoStateLegacyFromRepoState creates a new repoStateLegacy from a RepoState file. Only for testing.
func repoStateLegacyFromRepoState(rs *RepoState) *repoStateLegacy {
	return &repoStateLegacy{
		Head:     rs.Head,
		Remotes:  rs.Remotes,
		Backups:  rs.Backups,
		Branches: rs.Branches,
		Staged:   rs.staged,
		Working:  rs.working,
		Merge:    rs.merge,
	}
}

type mergeState struct {
	Commit          string `json:"commit"`
	PreMergeWorking string `json:"working_pre_merge"`
}

func (rs *repoStateLegacy) toRepoState() *RepoState {
	return &RepoState{
		Head:     rs.Head,
		Remotes:  rs.Remotes,
		Backups:  rs.Backups,
		Branches: rs.Branches,
		staged:   rs.Staged,
		working:  rs.Working,
		merge:    rs.Merge,
	}
}

func (rs *repoStateLegacy) save(fs filesys.ReadWriteFS) error {
	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return err
	}

	return fs.WriteFile(getRepoStateFile(), data)
}

// LoadRepoState parses the repo state file from the file system given
func LoadRepoState(fs filesys.ReadWriteFS) (*RepoState, error) {
	path := getRepoStateFile()
	data, err := fs.ReadFile(path)

	if err != nil {
		return nil, err
	}

	var repoState repoStateLegacy
	err = json.Unmarshal(data, &repoState)

	if err != nil {
		return nil, err
	}

	return repoState.toRepoState(), nil
}

func CloneRepoState(fs filesys.ReadWriteFS, r Remote) (*RepoState, error) {
	init := ref.NewBranchRef(DefaultInitBranch) // best effort
	hashStr := hash.Hash{}.String()
	rs := &RepoState{
		Head:     ref.MarshalableRef{Ref: init},
		staged:   hashStr,
		working:  hashStr,
		Remotes:  map[string]Remote{r.Name: r},
		Branches: make(map[string]BranchConfig),
		Backups:  make(map[string]Remote),
	}

	err := rs.Save(fs)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func CreateRepoState(fs filesys.ReadWriteFS, br string) (*RepoState, error) {
	headRef, err := ref.Parse(br)

	if err != nil {
		return nil, err
	}

	rs := &RepoState{
		Head:     ref.MarshalableRef{Ref: headRef},
		Remotes:  make(map[string]Remote),
		Branches: make(map[string]BranchConfig),
		Backups:  make(map[string]Remote),
	}

	err = rs.Save(fs)

	if err != nil {
		return nil, err
	}

	return rs, nil
}

// Save writes this repo state file to disk on the filesystem given
func (rs RepoState) Save(fs filesys.ReadWriteFS) error {
	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return err
	}

	return fs.WriteFile(getRepoStateFile(), data)
}

func (rs *RepoState) CWBHeadRef() ref.DoltRef {
	return rs.Head.Ref
}

func (rs *RepoState) CWBHeadSpec() *doltdb.CommitSpec {
	spec, _ := doltdb.NewCommitSpec("HEAD")
	return spec
}

func (rs *RepoState) AddRemote(r Remote) {
	rs.Remotes[r.Name] = r
}

func (rs *RepoState) RemoveRemote(r Remote) {
	delete(rs.Remotes, r.Name)
}

func (rs *RepoState) AddBackup(r Remote) {
	rs.Backups[r.Name] = r
}

func (rs *RepoState) RemoveBackup(r Remote) {
	delete(rs.Backups, r.Name)
}
