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

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type RepoStateReader interface {
	CWBHeadRef() ref.DoltRef
	CWBHeadSpec() *doltdb.CommitSpec
	WorkingHash() hash.Hash
	StagedHash() hash.Hash
	IsMergeActive() bool
	GetMergeCommit() string
}

type RepoStateWriter interface {
	// SetCWBHeadRef(context.Context, ref.DoltRef) error
	// SetCWBHeadSpec(context.Context, *doltdb.CommitSpec) error
	SetStagedHash(context.Context, hash.Hash) error
	SetWorkingHash(context.Context, hash.Hash) error
	ClearMerge() error
}

type DocsReadWriter interface {
	GetAllValidDocDetails() ([]doltdb.DocDetails, error)
	PutDocsToWorking(ctx context.Context, docDetails []doltdb.DocDetails) error
	PutDocsToStaged(ctx context.Context, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error)
	ResetWorkingDocsToStagedDocs(ctx context.Context) error
	GetDocDetail(docName string) (doc doltdb.DocDetails, err error)
}

type DbData struct {
	Ddb *doltdb.DoltDB
	Rsw RepoStateWriter
	Rsr RepoStateReader
	Drw DocsReadWriter
}

type BranchConfig struct {
	Merge  ref.MarshalableRef `json:"head"`
	Remote string             `json:"remote"`
}

type MergeState struct {
	Commit          string `json:"commit"`
	PreMergeWorking string `json:"working_pre_merge"`
}

type RepoState struct {
	Head     ref.MarshalableRef      `json:"head"`
	Staged   string                  `json:"staged"`
	Working  string                  `json:"working"`
	Merge    *MergeState             `json:"merge"`
	Remotes  map[string]Remote       `json:"remotes"`
	Branches map[string]BranchConfig `json:"branches"`
}

func LoadRepoState(fs filesys.ReadWriteFS) (*RepoState, error) {
	path := getRepoStateFile()
	data, err := fs.ReadFile(path)

	if err != nil {
		return nil, err
	}

	var repoState RepoState
	err = json.Unmarshal(data, &repoState)

	if err != nil {
		return nil, err
	}

	return &repoState, nil
}

func CloneRepoState(fs filesys.ReadWriteFS, r Remote) (*RepoState, error) {
	h := hash.Hash{}
	hashStr := h.String()
	rs := &RepoState{ref.MarshalableRef{
		Ref: ref.NewBranchRef("master")},
		hashStr,
		hashStr,
		nil,
		map[string]Remote{r.Name: r},
		make(map[string]BranchConfig),
	}

	err := rs.Save(fs)

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func CreateRepoState(fs filesys.ReadWriteFS, br string, rootHash hash.Hash) (*RepoState, error) {
	hashStr := rootHash.String()
	headRef, err := ref.Parse(br)

	if err != nil {
		return nil, err
	}

	rs := &RepoState{
		ref.MarshalableRef{Ref: headRef},
		hashStr,
		hashStr,
		nil,
		make(map[string]Remote),
		make(map[string]BranchConfig),
	}

	err = rs.Save(fs)

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (rs *RepoState) Save(fs filesys.ReadWriteFS) error {
	data, err := json.MarshalIndent(rs, "", "  ")

	if err != nil {
		return err
	}

	path := getRepoStateFile()

	return fs.WriteFile(path, data)
}

func (rs *RepoState) CWBHeadRef() ref.DoltRef {
	return rs.Head.Ref
}

func (rs *RepoState) CWBHeadSpec() *doltdb.CommitSpec {
	spec, _ := doltdb.NewCommitSpec("HEAD")
	return spec
}

func (rs *RepoState) StartMerge(commit string, fs filesys.Filesys) error {
	rs.Merge = &MergeState{commit, rs.Working}
	return rs.Save(fs)
}

func (rs *RepoState) AbortMerge(fs filesys.Filesys) error {
	rs.Working = rs.Merge.PreMergeWorking
	return rs.ClearMerge(fs)
}

func (rs *RepoState) ClearMerge(fs filesys.Filesys) error {
	rs.Merge = nil
	return rs.Save(fs)
}

func (rs *RepoState) AddRemote(r Remote) {
	rs.Remotes[r.Name] = r
}

func (rs *RepoState) WorkingHash() hash.Hash {
	return hash.Parse(rs.Working)
}

func (rs *RepoState) StagedHash() hash.Hash {
	return hash.Parse(rs.Staged)
}

func (rs *RepoState) IsMergeActive() bool {
	return rs.Merge != nil
}

func (rs *RepoState) GetMergeCommit() string {
	return rs.Merge.Commit
}

// Returns the working root.
func WorkingRoot(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (*doltdb.RootValue, error) {
	return ddb.ReadRootValue(ctx, rsr.WorkingHash())
}

// Updates the working root.
func UpdateWorkingRoot(ctx context.Context, ddb *doltdb.DoltDB, rsw RepoStateWriter, newRoot *doltdb.RootValue) (hash.Hash, error) {
	h, err := ddb.WriteRootValue(ctx, newRoot)

	if err != nil {
		return hash.Hash{}, doltdb.ErrNomsIO
	}

	err = rsw.SetWorkingHash(ctx, h)

	if err != nil {
		return hash.Hash{}, ErrStateUpdate
	}

	return h, nil
}

// Returns the head root.
func HeadRoot(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (*doltdb.RootValue, error) {
	commit, err := ddb.ResolveRef(ctx, rsr.CWBHeadRef())

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

// Returns the staged root.
func StagedRoot(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (*doltdb.RootValue, error) {
	return ddb.ReadRootValue(ctx, rsr.StagedHash())
}

// Updates the staged root.
func UpdateStagedRoot(ctx context.Context, ddb *doltdb.DoltDB, rsw RepoStateWriter, newRoot *doltdb.RootValue) (hash.Hash, error) {
	h, err := ddb.WriteRootValue(ctx, newRoot)

	if err != nil {
		return hash.Hash{}, doltdb.ErrNomsIO
	}

	err = rsw.SetStagedHash(ctx, h)

	if err != nil {
		return hash.Hash{}, ErrStateUpdate
	}

	return h, nil
}

func UpdateStagedRootWithVErr(ddb *doltdb.DoltDB, rsw RepoStateWriter, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	_, err := UpdateStagedRoot(context.Background(), ddb, rsw, updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the staged root state").Build()
	}

	return nil
}

func GetRoots(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (working *doltdb.RootValue, staged *doltdb.RootValue, head *doltdb.RootValue, err error) {
	working, err = WorkingRoot(ctx, ddb, rsr)

	if err != nil {
		return nil, nil, nil, err
	}

	staged, err = StagedRoot(ctx, ddb, rsr)

	if err != nil {
		return nil, nil, nil, err
	}

	head, err = HeadRoot(ctx, ddb, rsr)

	if err != nil {
		return nil, nil, nil, err
	}

	return working, staged, head, nil
}
