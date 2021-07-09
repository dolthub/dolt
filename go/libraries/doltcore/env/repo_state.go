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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/autoincr"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

type RepoStateReader interface {
	CWBHeadRef() ref.DoltRef
	CWBHeadSpec() *doltdb.CommitSpec
	// TODO: get rid of this
	IsMergeActive(ctx context.Context) (bool, error)
	// TODO: get rid of this
	GetMergeCommit(ctx context.Context) (*doltdb.Commit, error)
}

type RepoStateWriter interface {
	// TODO: get rid of this
	UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error
	// TODO: get rid of this
	UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error
	SetCWBHeadRef(context.Context, ref.MarshalableRef) error
	// TODO: get rid of this
	AbortMerge(ctx context.Context) error
	// TODO: get rid of this
	ClearMerge(ctx context.Context) error
	// TODO: get rid of this
	StartMerge(ctx context.Context, commit *doltdb.Commit) error
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
	Ait autoincr.AutoIncrementTracker
}

type BranchConfig struct {
	Merge  ref.MarshalableRef `json:"head"`
	Remote string             `json:"remote"`
}

type RepoState struct {
	Head     ref.MarshalableRef      `json:"head"`
	Remotes  map[string]Remote       `json:"remotes"`
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
	h := hash.Hash{}
	hashStr := h.String()
	rs := &RepoState{Head: ref.MarshalableRef{
		Ref: ref.NewBranchRef("master")},
		staged:   hashStr,
		working:  hashStr,
		Remotes:  map[string]Remote{r.Name: r},
		Branches: make(map[string]BranchConfig),
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

// Updates the working root.
func UpdateWorkingRoot(ctx context.Context, rsw RepoStateWriter, newRoot *doltdb.RootValue) error {
	// logrus.Infof("Updating working root with value %s", newRoot.DebugString(ctx, true))

	err := rsw.UpdateWorkingRoot(ctx, newRoot)
	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

// Returns the head root.
func HeadRoot(ctx context.Context, ddb *doltdb.DoltDB, rsr RepoStateReader) (*doltdb.RootValue, error) {
	commit, err := ddb.ResolveCommitRef(ctx, rsr.CWBHeadRef())

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

// Updates the staged root.
// TODO: remove this
func UpdateStagedRoot(ctx context.Context, rsw RepoStateWriter, newRoot *doltdb.RootValue) error {
	err := rsw.UpdateStagedRoot(ctx, newRoot)
	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

// TODO: this needs to be a function in the merge package, not repo state
func MergeWouldStompChanges(ctx context.Context, workingRoot *doltdb.RootValue, mergeCommit *doltdb.Commit, dbData DbData) ([]string, map[string]hash.Hash, error) {
	headRoot, err := HeadRoot(ctx, dbData.Ddb, dbData.Rsr)
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
	mergedHeadDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergedHeadDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

// GetGCKeepers queries |rsr| to find a list of values that need to be temporarily saved during GC.
// TODO: move this out of repo_state.go
func GetGCKeepers(ctx context.Context, env *DoltEnv) ([]hash.Hash, error) {
	workingRoot, err := env.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return nil, err
	}

	stagedRoot, err := env.StagedRoot(ctx)
	if err != nil {
		return nil, err
	}

	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return nil, err
	}

	keepers := []hash.Hash{
		workingHash,
		stagedHash,
	}

	mergeActive, err := env.IsMergeActive(ctx)
	if err != nil {
		return nil, err
	}

	if mergeActive {
		ws, err := env.WorkingSet(ctx)
		if err != nil {
			return nil, err
		}

		cm := ws.MergeState().Commit()
		ch, err := cm.HashOf()
		if err != nil {
			return nil, err
		}

		pmw := ws.MergeState().PreMergeWorkingRoot()
		pmwh, err := pmw.HashOf()
		if err != nil {
			return nil, err
		}

		keepers = append(keepers, ch, pmwh)
	}

	return keepers, nil
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
