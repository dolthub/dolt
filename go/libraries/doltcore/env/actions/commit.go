package actions

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/pkg/errors"
)

var ErrNameNotConfigured = errors.New("name not configured")
var ErrEmailNotConfigured = errors.New("email not configured")
var ErrEmptyCommitMessage = errors.New("commit message empty")

func getNameAndEmail(cfg *env.DoltCliConfig) (string, string, error) {
	name, err := cfg.GetString(env.UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(env.UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}

func CommitStaged(dEnv *env.DoltEnv, msg string) error {
	staged, notStaged, err := GetTableDiffs(dEnv)

	if msg == "" {
		return ErrEmptyCommitMessage
	}

	if err != nil {
		return err
	} else if len(staged.Tables) == 0 {
		return NothingStaged{notStaged}
	}

	name, email, err := getNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	var mergeCmSpec []*doltdb.CommitSpec
	if dEnv.IsMergeActive() {
		spec, err := doltdb.NewCommitSpec(dEnv.RepoState.Merge.Commit, dEnv.RepoState.Merge.Branch)

		if err != nil {
			panic("Corrupted repostate. Active merge state is not valid.")
		}

		mergeCmSpec = []*doltdb.CommitSpec{spec}
	}

	h := hash.Parse(dEnv.RepoState.Staged)
	meta := doltdb.NewCommitMeta(name, email, msg)
	_, err = dEnv.DoltDB.CommitWithParents(h, dEnv.RepoState.Branch, mergeCmSpec, meta)

	if err == nil {
		dEnv.RepoState.ClearMerge()
	}

	return err
}
