package env

import (
	"encoding/json"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

type MergeState struct {
	Branch          string `json:"branch"`
	Commit          string `json:"commit"`
	PreMergeWorking string `json:"working_pre_merge"`
}

type RepoState struct {
	Branch  string            `json:"branch"`
	Staged  string            `json:"staged"`
	Working string            `json:"working"`
	Merge   *MergeState       `json:"merge_state"`
	Remotes map[string]Remote `json:"remotes"`

	fs filesys.ReadWriteFS `json:"-"`
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
	rs := &RepoState{"", hashStr, hashStr, nil, map[string]Remote {r.Name: r}, fs}

	err := rs.Save()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func CreateRepoState(fs filesys.ReadWriteFS, br string, rootHash hash.Hash) (*RepoState, error) {
	hashStr := rootHash.String()
	rs := &RepoState{br, hashStr, hashStr, nil, nil, fs}

	err := rs.Save()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (rs *RepoState) Save() error {
	data, err := json.Marshal(rs)

	if err != nil {
		return err
	}

	path := getRepoStateFile()

	return rs.fs.WriteFile(path, data)
}

func (rs *RepoState) CWBHeadSpec() *doltdb.CommitSpec {
	spec, _ := doltdb.NewCommitSpec("HEAD", rs.Branch)

	return spec
}

func (rs *RepoState) StartMerge(branch, commit string) error {
	rs.Merge = &MergeState{branch, commit, rs.Working}
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
