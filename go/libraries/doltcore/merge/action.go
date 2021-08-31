package merge

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/hash"
	"time"
)


var ErrFailedToDetermineUnstagedDocs = errors.New("failed to determine unstaged docs")
var ErrFailedToReadDatabase = errors.New("failed to read database")
var ErrMergeFailedToUpdateDocs = errors.New("failed to update docs to the new working root")
var ErrMergeFailedToUpdateRepoState = errors.New("unable to execute repo state update")
var ErrFailedToDetermineMergeability = errors.New("failed to determine mergeability")

const (
	UserEmailKey = "user.email"
	UserNameKey  = "user.name"
	)

type MergeSpec struct {
	H1           hash.Hash
	H2           hash.Hash
	Cm1          *doltdb.Commit
	Cm2          *doltdb.Commit
	TblNames     []string
	WorkingDiffs map[string]hash.Hash
	Squash       bool
	Msg          string
	Noff         bool
	Force        bool
	AllowEmpty   bool
	Email        string
	Name         string
	Date         time.Time
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
	name, err := cfg.GetString(UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}

func ParseMergeSpec(ctx context.Context, dEnv *env.DoltEnv, msg string, commitSpecStr string, squash bool, noff bool, force bool, date time.Time) (*MergeSpec, bool, error) {
	cs1, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm1, err := dEnv.DoltDB.Resolve(context.TODO(), cs1, dEnv.RepoStateReader().CWBHeadRef())

	cs2, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm2, err := dEnv.DoltDB.Resolve(context.TODO(), cs2, dEnv.RepoStateReader().CWBHeadRef())

	h1, err := cm1.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err
	}

	h2, err := cm2.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err

	}

	//if h1 == h2 {
	//	return nil, false, doltdb.ErrUpToDate
	//}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return nil, false, err
	}

	tblNames, workingDiffs, err := MergeWouldStompChanges(ctx, roots, cm2)
	if err != nil {
		return nil, false, fmt.Errorf("%w; %s", ErrFailedToDetermineMergeability, err.Error())
	}

	name, email, err := GetNameAndEmail(dEnv.Config)
	if err != nil {
		//verr = errhand.BuildDError("error: committing").AddCause(err).Build()
		//return handleCommitErr(ctx, dEnv, verr, usage)
		return nil, false, err
	}

	return &MergeSpec{
		H1:           h1,
		H2:           h2,
		Cm1:          cm1,
		Cm2:          cm2,
		TblNames:     tblNames,
		WorkingDiffs: workingDiffs,
		Squash:       squash,
		Msg:          msg,
		Noff:         noff,
		Force:        force,
		Email:        email,
		Name:         name,
		Date:         date,
	}, true, nil
}

func MergeCommitSpec(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *MergeSpec) (map[string]*MergeStats, error) {

	if ok, err := mergeSpec.Cm1.CanFastForwardTo(ctx, mergeSpec.Cm2); ok {
		ancRoot, err := mergeSpec.Cm1.GetRootValue()
		if err != nil {
			return nil, err
		}
		mergedRoot, err := mergeSpec.Cm2.GetRootValue()
		if err != nil {
			return nil, err
		}
		if cvPossible, err := MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return nil, err
		} else if cvPossible {
			return ExecuteMerge(ctx, dEnv, mergeSpec)
		}
		if mergeSpec.Noff {
			return ExecNoFFMerge(ctx, dEnv, mergeSpec)
		} else {
			return nil, ExecuteFFMerge(ctx, dEnv, mergeSpec)
		}
	} else if err == doltdb.ErrUpToDate || err == doltdb.ErrIsAhead {
		return nil, err
	} else {
		return ExecuteMerge(ctx, dEnv, mergeSpec)
	}
}

func ExecNoFFMerge(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *MergeSpec) (map[string]*MergeStats, error) {
	mergedRoot, err := mergeSpec.Cm2.GetRootValue()

	if err != nil {
		//return errhand.BuildDError("error: reading from database").AddCause(err).Build()
		return nil, ErrFailedToReadDatabase
	}

	tblToStats := make(map[string]*MergeStats)
	err = mergedRootToWorking(ctx, false, dEnv, mergedRoot, mergeSpec.WorkingDiffs, mergeSpec.Cm2, tblToStats)

	if err != nil {
		return tblToStats, err
	}

	// Reload roots since the above method writes new values to the working set
	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return tblToStats, err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return tblToStats, err
	}

	var mergeParentCommits []*doltdb.Commit
	if ws.MergeActive() {
		mergeParentCommits = []*doltdb.Commit{ws.MergeState().Commit()}
	}

	_, err = actions.CommitStaged(ctx, roots, ws.MergeActive(), mergeParentCommits, dEnv.DbData(), actions.CommitStagedProps{
		Message:    mergeSpec.Msg,
		Date:       mergeSpec.Date,
		AllowEmpty: mergeSpec.AllowEmpty,
		Force:      mergeSpec.Force,
		Name:       mergeSpec.Name,
		Email:      mergeSpec.Email,
	})

	if err != nil {
		//return errhand.BuildDError("error: committing").AddCause(err).Build()
		return tblToStats, fmt.Errorf("%w; failed to commit", err)
	}

	err = dEnv.ClearMerge(ctx)
	if err != nil {
		return tblToStats, err
	}

	return tblToStats, err
}

func applyChanges(ctx context.Context, root *doltdb.RootValue, workingDiffs map[string]hash.Hash) (*doltdb.RootValue, error) {
	var err error
	for tblName, h := range workingDiffs {
		root, err = root.SetTableHash(ctx, tblName, h)

		if err != nil {
			//return nil, errhand.BuildDError("error: Failed to update table '%s'.", tblName).AddCause(err).Build()
			return nil, fmt.Errorf("failed to update table; %w", err)
		}
	}

	return root, nil
}

func ExecuteFFMerge(
	ctx context.Context,
	dEnv *env.DoltEnv,
	mergeSpec *MergeSpec,
) error {
	stagedRoot, err := mergeSpec.Cm2.GetRootValue()
	if err != nil {
		//return errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
		return err
	}

	workingRoot := stagedRoot
	if len(mergeSpec.WorkingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, stagedRoot, mergeSpec.WorkingDiffs)

		if err != nil {
			//return errhand.BuildDError("Failed to re-apply working changes.").AddCause(err).Build()
			return err
		}
	}

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: unable to determine unstaged docs").AddCause(err).Build()
		return err
	}

	if !mergeSpec.Squash {
		err = dEnv.DoltDB.FastForward(ctx, dEnv.RepoStateReader().CWBHeadRef(), mergeSpec.Cm2)

		if err != nil {
			//return errhand.BuildDError("Failed to write database").AddCause(err).Build()
			return err
		}
	}

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, workingSet.WithWorkingRoot(workingRoot).WithStagedRoot(stagedRoot))
	if err != nil {
		//		return errhand.BuildDError("unable to execute repo state update.").
		//			AddDetails(`As a result your .dolt/repo_state.json file may have invalid values for "staged" and "working".
		//At the moment the best way to fix this is to run:
		//
		//    dolt branch -v
		//
		//and take the hash for your current branch and use it for the value for "staged" and "working"`).
		//			AddCause(err).Build()
		return ErrMergeFailedToUpdateRepoState
	}

	err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs)
	if err != nil {
		//return errhand.BuildDError("error: failed to update docs to the new working root").AddCause(err).Build()
		return ErrMergeFailedToUpdateDocs
	}

	return nil
}

func ExecuteMerge(ctx context.Context, dEnv *env.DoltEnv, mergeSpec *MergeSpec) (map[string]*MergeStats, error) {
	mergedRoot, tblToStats, err := MergeCommits(ctx, mergeSpec.Cm1, mergeSpec.Cm2)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return tblToStats, fmt.Errorf("already up to date; %w", err)
		case ErrFastForward:
			panic("fast forward merge")
		}
		return tblToStats, err
	}
	//if err != nil {
	//	switch err {
	//	case doltdb.ErrUpToDate:
	//		return errhand.BuildDError("Already up to date.").AddCause(err).Build()
	//	case merge.ErrFastForward:
	//		panic("fast forward merge")
	//	default:
	//		return errhand.BuildDError("Bad merge").AddCause(err).Build()
	//	}
	//}

	return tblToStats, mergedRootToWorking(ctx, mergeSpec.Squash, dEnv, mergedRoot, mergeSpec.WorkingDiffs, mergeSpec.Cm2, tblToStats)
}

// TODO: change this to be functional and not write to repo state
func mergedRootToWorking(
	ctx context.Context,
	squash bool,
	dEnv *env.DoltEnv,
	mergedRoot *doltdb.RootValue,
	workingDiffs map[string]hash.Hash,
	cm2 *doltdb.Commit,
	tblToStats map[string]*MergeStats,
) error {
	var err error

	workingRoot := mergedRoot
	if len(workingDiffs) > 0 {
		workingRoot, err = applyChanges(ctx, mergedRoot, workingDiffs)

		if err != nil {
			return err
		}
	}

	if !squash {
		err = dEnv.StartMerge(ctx, cm2)

		if err != nil {
			//return errhand.BuildDError("Unable to update the repo state").AddCause(err).Build()
			return actions.ErrFailedToSaveRepoState
		}
	}

	unstagedDocs, err := actions.GetUnstagedDocs(ctx, dEnv)
	if err != nil {
		//return errhand.BuildDError("error: failed to determine unstaged docs").AddCause(err).Build()
		return ErrFailedToDetermineUnstagedDocs
	}

	//verr := UpdateWorkingWithVErr(dEnv, workingRoot)
	err = dEnv.UpdateWorkingRoot(context.Background(), workingRoot)
	if err != nil {
		return err
	}

	conflicts, constraintViolations := conflictsAndViolations(tblToStats)
	if len(conflicts) > 0 || len(constraintViolations) > 0 {
		return err
	}

	if err = actions.SaveDocsFromWorkingExcludingFSChanges(ctx, dEnv, unstagedDocs); err != nil {
		return err
	}

	if err = dEnv.UpdateStagedRoot(context.Background(), mergedRoot); err != nil {
		return err
	}

	return nil
}

func conflictsAndViolations(tblToStats map[string]*MergeStats) (conflicts []string, constraintViolations []string) {
	for tblName, stats := range tblToStats {
		if stats.Operation == TableModified && (stats.Conflicts > 0 || stats.ConstraintViolations > 0) {
			if stats.Conflicts > 0 {
				conflicts = append(conflicts, tblName)
			}
			if stats.ConstraintViolations > 0 {
				constraintViolations = append(constraintViolations, tblName)
			}
		}
	}
	return
}
