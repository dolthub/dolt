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
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type RepoStateReader interface {
	CWBHeadRef() ref.DoltRef
	CWBHeadSpec() *doltdb.CommitSpec
	CWBHeadHash(ctx context.Context) (hash.Hash, error)
	WorkingHash() hash.Hash
	StagedHash() hash.Hash
	IsMergeActive() bool
	GetMergeCommit() string
	GetPreMergeWorking() string
}

type RepoStateWriter interface {
	// SetCWBHeadSpec(context.Context, *doltdb.CommitSpec) error
	SetStagedHash(context.Context, hash.Hash) error
	SetWorkingHash(context.Context, hash.Hash) error
	SetCWBHeadRef(context.Context, ref.MarshalableRef) error
	AbortMerge() error
	ClearMerge() error
	StartMerge(commitStr string) error
}

type DocsReadWriter interface {
	// GetDocsOnDisk returns the docs in the filesytem optionally filtered by docNames.
	GetDocsOnDisk(docNames ...string) (doltdocs.Docs, error)
	// WriteDocsToDisk updates the documents stored in the filesystem with the contents in docs.
	WriteDocsToDisk(docs doltdocs.Docs) error
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

func MergeWouldStompChanges(ctx context.Context, mergeCommit *doltdb.Commit, dbData DbData) ([]string, map[string]hash.Hash, error) {
	headRoot, err := HeadRoot(ctx, dbData.Ddb, dbData.Rsr)

	if err != nil {
		return nil, nil, err
	}

	workingRoot, err := WorkingRoot(ctx, dbData.Ddb, dbData.Rsr)

	if err != nil {
		return nil, nil, err
	}

	mergeRoot, err := mergeCommit.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := mapTableHashes(ctx, headRoot)

	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := mapTableHashes(ctx, workingRoot)

	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mapTableHashes(ctx, mergeRoot)

	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergeWorkingDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergeWorkingDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func ResolveMergeCommitHash(ctx context.Context, rsr RepoStateReader, ddb *doltdb.DoltDB) (h hash.Hash, err error) {
	spec, err := doltdb.NewCommitSpec(rsr.GetMergeCommit())
	if err != nil {
		return h, err
	}

	cm, err := ddb.Resolve(ctx, spec, nil)
	if err != nil {
		return h, err
	}

	return cm.HashOf()
}

func ResolvePreMergeWorkingRoot(ctx context.Context, rsr RepoStateReader, ddb *doltdb.DoltDB) (h hash.Hash, err error) {
	h = hash.Parse(rsr.GetPreMergeWorking())

	val, err := ddb.ValueReadWriter().ReadValue(ctx, h)
	if err != nil {
		return h, err
	}
	if val == nil {
		return h, fmt.Errorf("MergeState.PreMergeWorking is a dangling hash")
	}

	return h, nil
}

func mapTableHashes(ctx context.Context, root *doltdb.RootValue) (map[string]hash.Hash, error) {
	names, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	nameToHash := make(map[string]hash.Hash)
	for _, name := range names {
		h, ok, err := root.GetTableHash(ctx, name)

		if err != nil {
			return nil, err
		} else if !ok {
			panic("GetTableNames returned a table that GetTableHash says isn't there.")
		} else {
			nameToHash[name] = h
		}
	}

	return nameToHash, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}
