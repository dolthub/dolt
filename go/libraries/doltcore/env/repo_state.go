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
	WorkingHash() hash.Hash
	StagedHash() hash.Hash
	IsMergeActive() bool
	GetMergeCommit() string
}

type RepoStateWriter interface {
	// SetCWBHeadRef(context.Context, ref.DoltRef) error
	// SetCWBHeadSpec(context.Context, *doltdb.CommitSpec) error
	SetWorkingHash(context.Context, hash.Hash) error
	UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) (hash.Hash, error)
	ClearMerge() error
	//	SetStagedHash(context.Context, hash.Hash) error
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

func (rs* RepoState) IsMergeActive() bool {
	return rs.Merge != nil
}

func (rs* RepoState) GetMergeCommit() string {
	return rs.Merge.Commit
}

func HeadRoot(ctx context.Context, ddb *doltdb.DoltDB, reader RepoStateReader) (*doltdb.RootValue, error) {
	commit, err := ddb.ResolveRef(ctx, reader.CWBHeadRef())

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

func StagedRoot(ctx context.Context, ddb *doltdb.DoltDB, reader RepoStateReader) (*doltdb.RootValue, error) {
	return ddb.ReadRootValue(ctx, reader.StagedHash())
}


func WorkingRoot(ctx context.Context, ddb *doltdb.DoltDB, reader RepoStateReader) (*doltdb.RootValue, error) {
	return ddb.ReadRootValue(ctx, reader.WorkingHash())
}

func UpdateWorkingRoot(ctx context.Context, ddb *doltdb.DoltDB, writer RepoStateWriter, newRoot *doltdb.RootValue) error {
	h, err := ddb.WriteRootValue(ctx, newRoot)

	if err != nil {
		return doltdb.ErrNomsIO
	}

	return writer.SetWorkingHash(ctx, h)
}
