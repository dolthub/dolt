package env

import (
	"encoding/json"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type BranchConfig struct {
	Merge  ref.MarshalableRef `json:"head"`
	Remote string             `json:"remote"`
}

type MergeState struct {
	Head            ref.MarshalableRef `json:"head"`
	Commit          string             `json:"commit"`
	PreMergeWorking string             `json:"working_pre_merge"`
}

type RepoState struct {
	Head     ref.MarshalableRef      `json:"head"`
	Staged   string                  `json:"staged"`
	Working  string                  `json:"working"`
	Merge    *MergeState             `json:"merge"`
	Remotes  map[string]Remote       `json:"remotes"`
	Branches map[string]BranchConfig `json:"branches"`

	fs filesys.ReadWriteFS
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

	repoState.fs = fs

	return &repoState, nil
}

func CloneRepoState(fs filesys.ReadWriteFS, r Remote) (*RepoState, error) {
	h := hash.Hash{}
	hashStr := h.String()
	rs := &RepoState{ref.MarshalableRef{Ref: ref.NewBranchRef("master")}, hashStr, hashStr, nil, map[string]Remote{r.Name: r}, nil, fs}

	err := rs.Save()

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

	rs := &RepoState{ref.MarshalableRef{Ref: headRef}, hashStr, hashStr, nil, nil, nil, fs}

	err = rs.Save()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (rs *RepoState) Save() error {
	data, err := json.MarshalIndent(rs, "", "  ")

	if err != nil {
		return err
	}

	path := getRepoStateFile()

	return rs.fs.WriteFile(path, data)
}

func (rs *RepoState) CWBHeadSpec() *doltdb.CommitSpec {
	spec, _ := doltdb.NewCommitSpec("HEAD", rs.Head.Ref.String())

	return spec
}

func (rs *RepoState) StartMerge(dref ref.DoltRef, commit string) error {
	rs.Merge = &MergeState{ref.MarshalableRef{Ref: dref}, commit, rs.Working}
	return rs.Save()
}

func (rs *RepoState) AbortMerge() error {
	rs.Working = rs.Merge.PreMergeWorking
	return rs.ClearMerge()
}

func (rs *RepoState) ClearMerge() error {
	rs.Merge = nil
	return rs.Save()
}

func (rs *RepoState) AddRemote(r Remote) {
	if rs.Remotes == nil {
		rs.Remotes = make(map[string]Remote)
	}

	rs.Remotes[r.Name] = r
}
