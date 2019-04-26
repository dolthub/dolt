package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
)

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

func ResolveCommitWithVErr(dEnv *env.DoltEnv, cSpecStr, cwb string) (*doltdb.Commit, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(cSpecStr, cwb)

	if err != nil {
		return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		if err == doltdb.ErrInvalidAnscestorSpec {
			return nil, errhand.BuildDError("'%s' is an invalid ancestor spec", cs.AncestorSpec().SpecStr).Build()
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
	cm, err := actions.MaybeGetCommit(dEnv, maybeCommit)

	if err != nil {
		bdr := errhand.BuildDError("fatal: Unable to read from data repository.")
		return nil, bdr.AddCause(err).Build()
	}

	return cm, nil
}
