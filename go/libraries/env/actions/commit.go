package actions

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/liquidata-inc/ld/dolt/go/libraries/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/pkg/errors"
)

var ErrNameNotConfigured = errors.New("name not configured")
var ErrEmailNotConfigured = errors.New("email not configured")

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

	if err != nil {
		return err
	} else if len(staged.Tables) == 0 {
		return NothingStaged{notStaged}
	}

	name, email, err := getNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	h := hash.Parse(dEnv.RepoState.Staged)
	meta := doltdb.NewCommitMeta(name, email, msg)
	_, err = dEnv.DoltDB.Commit(h, dEnv.RepoState.Branch, meta)

	return err
}
