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

package commands

import (
	"context"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
)

var fwtStageName = "fwt"

func GetWorkingWithVErr(dEnv *env.DoltEnv) (*doltdb.RootValue, errhand.VerboseError) {
	working, err := dEnv.WorkingRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get working.").AddCause(err).Build()
	}

	return working, nil
}

func GetStagedWithVErr(dEnv *env.DoltEnv) (*doltdb.RootValue, errhand.VerboseError) {
	staged, err := dEnv.StagedRoot(context.Background())

	if err != nil {
		return nil, errhand.BuildDError("Unable to get staged.").AddCause(err).Build()
	}

	return staged, nil
}

func UpdateWorkingWithVErr(dEnv *env.DoltEnv, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	err := dEnv.UpdateWorkingRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the working root state").Build()
	}

	return nil
}

func UpdateStagedWithVErr(dEnv *env.DoltEnv, updatedRoot *doltdb.RootValue) errhand.VerboseError {
	_, err := dEnv.UpdateStagedRoot(context.Background(), updatedRoot)

	switch err {
	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write value").Build()
	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to update the staged root state").Build()
	}

	return nil
}

func ValidateTablesWithVErr(tbls []string, roots ...*doltdb.RootValue) errhand.VerboseError {
	err := actions.ValidateTables(context.TODO(), tbls, roots...)

	if err != nil {
		if actions.IsTblNotExist(err) {
			tbls := actions.GetTablesForError(err)
			bdr := errhand.BuildDError("Invalid Table(s):")

			for _, tbl := range tbls {
				bdr.AddDetails("\t" + tbl)
			}

			return bdr.Build()
		} else {
			return errhand.BuildDError("fatal: " + err.Error()).Build()
		}
	}

	return nil
}

func ResolveCommitWithVErr(dEnv *env.DoltEnv, cSpecStr string) (*doltdb.Commit, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(cSpecStr)

	if err != nil {
		return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(context.TODO(), cs, dEnv.RepoState.CWBHeadRef())

	if err != nil {
		if err == doltdb.ErrInvalidAncestorSpec {
			return nil, errhand.BuildDError("'%s' could not resolve ancestor spec", cSpecStr).Build()
		} else if doltdb.IsNotFoundErr(err) {
			return nil, errhand.BuildDError("'%s' not found", cSpecStr).Build()
		} else if err == doltdb.ErrFoundHashNotACommit {
			return nil, errhand.BuildDError("'%s' is not a commit", cSpecStr).Build()
		} else {
			return nil, errhand.BuildDError("Unexpected error resolving '%s'", cSpecStr).AddCause(err).Build()
		}
	}

	return cm, nil
}

func MaybeGetCommitWithVErr(dEnv *env.DoltEnv, maybeCommit string) (*doltdb.Commit, errhand.VerboseError) {
	cm, err := actions.MaybeGetCommit(context.TODO(), dEnv, maybeCommit)

	if err != nil {
		bdr := errhand.BuildDError("fatal: Unable to read from data repository.")
		return nil, bdr.AddCause(err).Build()
	}

	return cm, nil
}
