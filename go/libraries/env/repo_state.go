package env

import (
	"encoding/json"
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
)

type RepoState struct {
	Branch  string `json:"branch"`
	Staged  string `json:"staged"`
	Working string `json:"working"`

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

func CreateRepoState(fs filesys.ReadWriteFS, br string, rootHash hash.Hash) (*RepoState, error) {
	hashStr := rootHash.String()
	rs := &RepoState{br, hashStr, hashStr, fs}

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
