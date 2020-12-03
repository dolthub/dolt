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

package actions

import (
	"context"
	"errors"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/hash"
)

var ErrNameNotConfigured = errors.New("name not configured")
var ErrEmailNotConfigured = errors.New("email not configured")
var ErrEmptyCommitMessage = errors.New("commit message empty")

type CommitStagedProps struct {
	Message          string
	Date             time.Time
	AllowEmpty       bool
	CheckForeignKeys bool
	Name             string
	Email            string
}

// we are more permissive than what is documented.
var SupportedLayouts = []string{
	"2006/01/02",
	"2006/01/02T15:04:05",
	"2006/01/02T15:04:05Z07:00",

	"2006.01.02",
	"2006.01.02T15:04:05",
	"2006.01.02T15:04:05Z07:00",

	"2006-01-02",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}


// Parses a date string. Used by multiple commands.
func ParseDate(dateStr string) (time.Time, error) {
	for _, layout := range SupportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("error: '" + dateStr + "' is not in a supported format.")
}

// Parses the author flag for the commit method.
func ParseAuthor(authorStr string) (string, string, error) {
	if len(authorStr) == 0 {
		return "", "", errors.New("Option 'author' requires a value")
	}

	reg := regexp.MustCompile("(?m)([^)]+) \\<([^)]+)") // Regex matches Name <email
	matches := reg.FindStringSubmatch(authorStr)        // This function places the original string at the beginning of matches

	// If name and email are provided
	if len(matches) != 3 {
		return "", "", errors.New("Author not formatted correctly. Use 'Name <author@example.com>' format")
	}

	name := matches[1]
	email := strings.ReplaceAll(matches[2], ">", "")

	return name, email, nil
}

const (
	AllowEmptyFlag   = "allow-empty"
	DateParam        = "date"
	CommitMessageArg = "message"
	AuthorParam      = "author"
	ForceFlag		 = "force"
)

// Creates the argparser shared dolt commit cli and DOLT_COMMIT>
func CreateCommitArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(CommitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AllowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(DateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor <author@example.com> format.")
	return ap
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
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

func CommitStaged(ctx context.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader, rsw env.RepoStateWriter, props CommitStagedProps) (string, error) {
	if props.Message == "" {
		return "", ErrEmptyCommitMessage
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, ddb, rsr)
	if err != nil {
		return "", err
	}

	var stagedTblNames []string
	for _, td := range staged {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	if len(staged) == 0 && !rsr.IsMergeActive() && !props.AllowEmpty {
		_, notStagedDocs, err := diff.GetDocDiffs(ctx, ddb, rsr)
		if err != nil {
			return "", err
		}
		return "", NothingStaged{notStaged, notStagedDocs}
	}

	var mergeCmSpec []*doltdb.CommitSpec
	if rsr.IsMergeActive() {
		root, err := env.WorkingRoot(ctx, ddb, rsr)
		if err != nil {
			return "", err
		}
		inConflict, err := root.TablesInConflict(ctx)
		if err != nil {
			return "", err
		}
		if len(inConflict) > 0 {
			return "", NewTblInConflictError(inConflict)
		}

		spec, err := doltdb.NewCommitSpec(rsr.GetMergeCommit())

		if err != nil {
			panic("Corrupted repostate. Active merge state is not valid.")
		}

		mergeCmSpec = []*doltdb.CommitSpec{spec}
	}

	srt, err := env.StagedRoot(ctx, ddb, rsr)

	if err != nil {
		return "", err
	}

	srt, err = srt.UpdateSuperSchemasFromOther(ctx, stagedTblNames, srt)

	if err != nil {
		return "", err
	}

	if props.CheckForeignKeys {
		srt, err = srt.ValidateForeignKeys(ctx)
		if err != nil {
			return "", err
		}
	}

	h, err := rsw.UpdateStagedRoot(ctx, srt)

	if err != nil {
		return "", err
	}

	wrt, err := env.WorkingRoot(ctx, ddb, rsr)

	if err != nil {
		return "", err
	}

	wrt, err = wrt.UpdateSuperSchemasFromOther(ctx, stagedTblNames, srt)

	if err != nil {
		return "", err
	}

	err = env.UpdateWorkingRoot(ctx, ddb, rsw, wrt)

	if err != nil {
		return "", err
	}

	meta, noCommitMsgErr := doltdb.NewCommitMetaWithUserTS(props.Name, props.Email, props.Message, props.Date)

	if noCommitMsgErr != nil {
		return "", ErrEmptyCommitMessage
	}

	// DoltDB resolves the current working branch head ref to provide a parent commit.
	// Any commit specs in mergeCmSpec are also resolved and added.
	c, err := ddb.CommitWithParentSpecs(ctx, h, rsr.CWBHeadRef(), mergeCmSpec, meta)

	if err == nil {
		err = rsw.ClearMerge()
	}

	h, err = c.HashOf()

	return h.String(), err
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

	if _, ok := hashToCommit[hash]; ok {
		return nil
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
