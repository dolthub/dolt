// Copyright 2019 Liquidata, Inc.
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

package actions

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/dolt/go/store/hash"
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

func CommitStaged(ctx context.Context, dEnv *env.DoltEnv, msg string, date time.Time, allowEmpty bool) error {
	stagedTbls, notStagedTbls, err := GetTableDiffs(ctx, dEnv)

	if msg == "" {
		return ErrEmptyCommitMessage
	}

	if err != nil {
		return err
	}

	_, notStagedDocs, err := GetDocDiffs(ctx, dEnv)

	if err != nil {
		return err
	}

	if len(stagedTbls.Tables) == 0 && dEnv.RepoState.Merge == nil && !allowEmpty {
		return NothingStaged{notStagedTbls, notStagedDocs}
	}

	name, email, err := getNameAndEmail(dEnv.Config)

	if err != nil {
		return err
	}

	var mergeCmSpec []*doltdb.CommitSpec
	if dEnv.IsMergeActive() {
		spec, err := doltdb.NewCommitSpec(dEnv.RepoState.Merge.Commit, dEnv.RepoState.Merge.Head.Ref.String())

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

	meta, noCommitMsgErr := doltdb.NewCommitMetaWithUserTS(name, email, msg, date)
	if noCommitMsgErr != nil {
		return ErrEmptyCommitMessage
	}

	_, err = dEnv.DoltDB.CommitWithParents(ctx, h, dEnv.RepoState.CWBHeadRef(), mergeCmSpec, meta)

	if err == nil {
		dEnv.RepoState.ClearMerge(dEnv.FS)
	}

	return err
}

// TimeSortedCommits returns a reverse-chronological (latest-first) list of the most recent `n` ancestors of `commit`.
// Passing a negative value for `n` will result in all ancestors being returned.
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

	var sortErr error
	var metaI, metaJ *doltdb.CommitMeta
	sort.Slice(uniqueCommits, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		metaI, sortErr = uniqueCommits[i].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		metaJ, sortErr = uniqueCommits[j].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		return metaI.UserTimestamp > metaJ.UserTimestamp
	})

	if sortErr != nil {
		return nil, sortErr
	}

	return uniqueCommits, nil
}

func AddCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, hashToCommit map[hash.Hash]*doltdb.Commit, n int) error {
	hash, err := commit.HashOf()

	if err != nil {
		return err
	}

	hashToCommit[hash] = commit

	numParents, err := commit.NumParents()

	if err != nil {
		return err
	}

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
