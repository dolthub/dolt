package actions

import (
	"context"
	"sort"

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

func CommitStaged(ctx context.Context, dEnv *env.DoltEnv, msg string, allowEmpty bool) error {
	staged, notStaged, err := GetTableDiffs(ctx, dEnv)

	if msg == "" {
		return ErrEmptyCommitMessage
	}

	if err != nil {
		return err
	}

	if len(staged.Tables) == 0 && dEnv.RepoState.Merge == nil && !allowEmpty {
		return NothingStaged{notStaged}
	}

	name, email, err := getNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	var mergeCmSpec []*doltdb.CommitSpec
	if dEnv.IsMergeActive() {
		spec, err := doltdb.NewCommitSpec(dEnv.RepoState.Merge.Commit, dEnv.RepoState.Merge.Head.String())

		if err != nil {
			panic("Corrupted repostate. Active merge state is not valid.")
		}

		mergeCmSpec = []*doltdb.CommitSpec{spec}
	}

	root, err := dEnv.StagedRoot(ctx)

	if err != nil {
		return err
	}

	h, err := dEnv.UpdateStagedRoot(ctx, root)

	if err != nil {
		return err
	}

	meta, noCommitMsgErr := doltdb.NewCommitMeta(name, email, msg)
	if noCommitMsgErr != nil {
		return ErrEmptyCommitMessage
	}

	_, err = dEnv.DoltDB.CommitWithParents(ctx, h, dEnv.RepoState.Head, mergeCmSpec, meta)

	if err == nil {
		dEnv.RepoState.ClearMerge()
	}

	return err
}

func TimeSortedCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, n int) ([]*doltdb.Commit, error) {
	hashToCommit := make(map[hash.Hash]*doltdb.Commit)
	err := AddCommits(ctx, ddb, commit, hashToCommit, n)

	if err != nil {
		return nil, err
	}

	idx := 0
	uniqueCommits := make([]*doltdb.Commit, len(hashToCommit))
	for _, v := range hashToCommit {
		uniqueCommits[idx] = v
		idx++
	}

	sort.Slice(uniqueCommits, func(i, j int) bool {
		return uniqueCommits[i].GetCommitMeta().Timestamp > uniqueCommits[j].GetCommitMeta().Timestamp
	})

	return uniqueCommits, nil
}

func AddCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, hashToCommit map[hash.Hash]*doltdb.Commit, n int) error {
	hash := commit.HashOf()
	hashToCommit[hash] = commit

	numParents := commit.NumParents()
	for i := 0; i < numParents && len(hashToCommit) != n; i++ {
		parentCommit, err := ddb.ResolveParent(ctx, commit, i)

		if err != nil {
			return err
		}

		err = AddCommits(ctx, ddb, parentCommit, hashToCommit, n)

		if err != nil {
			return err
		}
	}

	return nil
}
