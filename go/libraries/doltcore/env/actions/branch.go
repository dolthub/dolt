package actions

import (
	"context"
	"errors"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/math"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

var ErrAlreadyExists = errors.New("already exists")
var ErrCOBranchDelete = errors.New("attempted to delete checked out branch")
var ErrUnmergedBranchDelete = errors.New("attempted to delete a branch that is not fully merged into master; use `-f` to force")

func MoveBranch(ctx context.Context, dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	err := CopyBranch(dEnv, oldBranch, newBranch, force)

	if err != nil {
		return err
	}

	if dEnv.RepoState.Branch == oldBranch {
		dEnv.RepoState.Branch = newBranch
		err = dEnv.RepoState.Save()

		if err != nil {
			return err
		}
	}

	return DeleteBranch(ctx, dEnv, oldBranch, true)
}

func CopyBranch(dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	if !dEnv.DoltDB.HasBranch(oldBranch) {
		return doltdb.ErrBranchNotFound
	} else if !force && dEnv.DoltDB.HasBranch(newBranch) {
		return ErrAlreadyExists
	} else if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, _ := doltdb.NewCommitSpec("head", oldBranch)
	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewBranchAtCommit(newBranch, cm)
}

func DeleteBranch(ctx context.Context, dEnv *env.DoltEnv, brName string, force bool) error {
	if !dEnv.DoltDB.HasBranch(brName) {
		return doltdb.ErrBranchNotFound
	} else if dEnv.RepoState.Branch == brName {
		return ErrCOBranchDelete
	}

	ms, err := doltdb.NewCommitSpec("head", "master")

	if err != nil {
		return err
	}

	master, err := dEnv.DoltDB.Resolve(ms)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec("head", brName)

	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return err
	}

	if !force {
		if isMerged, _ := master.CanFastReverseTo(ctx, cm); !isMerged {
			return ErrUnmergedBranchDelete
		}
	}

	return dEnv.DoltDB.DeleteBranch(brName)
}

func CreateBranch(dEnv *env.DoltEnv, newBranch, startingPoint string, force bool) error {
	if !force && dEnv.DoltDB.HasBranch(newBranch) {
		return ErrAlreadyExists
	}

	if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, err := doltdb.NewCommitSpec(startingPoint, dEnv.RepoState.Branch)

	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewBranchAtCommit(newBranch, cm)
}

func CheckoutBranch(ctx context.Context, dEnv *env.DoltEnv, brName string) error {
	if !dEnv.DoltDB.HasBranch(brName) {
		return doltdb.ErrBranchNotFound
	}

	if dEnv.RepoState.Branch == brName {
		return doltdb.ErrAlreadyOnBranch
	}

	currRoots, err := getRoots(ctx, dEnv, HeadRoot, WorkingRoot, StagedRoot)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec("head", brName)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	newRoot := cm.GetRootValue()
	conflicts := set.NewStrSet([]string{})
	wrkTblHashes := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[WorkingRoot], conflicts)
	stgTblHashes := tblHashesForCO(ctx, currRoots[HeadRoot], newRoot, currRoots[StagedRoot], conflicts)

	if conflicts.Size() > 0 {
		return CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	wrkHash, err := writeRoot(ctx, dEnv, wrkTblHashes)

	if err != nil {
		return err
	}

	stgHash, err := writeRoot(ctx, dEnv, stgTblHashes)

	if err != nil {
		return err
	}

	dEnv.RepoState.Branch = brName
	dEnv.RepoState.Working = wrkHash.String()
	dEnv.RepoState.Staged = stgHash.String()
	dEnv.RepoState.Save()

	return nil
}

var emptyHash = hash.Hash{}

func tblHashesForCO(ctx context.Context, oldRoot, newRoot, changedRoot *doltdb.RootValue, conflicts *set.StrSet) map[string]hash.Hash {
	resultMap := make(map[string]hash.Hash)
	for _, tblName := range newRoot.GetTableNames(ctx) {
		oldHash, _ := oldRoot.GetTableHash(ctx, tblName)
		newHash, _ := newRoot.GetTableHash(ctx, tblName)
		changedHash, _ := changedRoot.GetTableHash(ctx, tblName)

		if oldHash == changedHash {
			resultMap[tblName] = newHash
		} else if oldHash == newHash {
			resultMap[tblName] = changedHash
		} else if newHash == changedHash {
			resultMap[tblName] = oldHash
		} else {
			conflicts.Add(tblName)
		}
	}

	for _, tblName := range changedRoot.GetTableNames(ctx) {
		if _, exists := resultMap[tblName]; !exists {
			oldHash, _ := oldRoot.GetTableHash(ctx, tblName)
			changedHash, _ := changedRoot.GetTableHash(ctx, tblName)

			if oldHash == emptyHash {
				resultMap[tblName] = changedHash
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}
	}

	return resultMap
}

func writeRoot(ctx context.Context, dEnv *env.DoltEnv, tblHashes map[string]hash.Hash) (hash.Hash, error) {
	for k, v := range tblHashes {
		if v == emptyHash {
			delete(tblHashes, k)
		}
	}

	root, err := doltdb.NewRootValue(ctx, dEnv.DoltDB.ValueReadWriter(), tblHashes)

	if err != nil {
		if err == doltdb.ErrHashNotFound {
			return emptyHash, errors.New("corrupted database? Can't find hash of current table")
		}
		return emptyHash, doltdb.ErrNomsIO
	}

	return dEnv.DoltDB.WriteRootValue(ctx, root)
}

func getDifferingTables(ctx context.Context, root1, root2 *doltdb.RootValue) []string {
	tbls := root1.GetTableNames(ctx)
	differing := make([]string, 0, len(tbls))
	for _, tbl := range tbls {
		hsh1, _ := root1.GetTableHash(ctx, tbl)
		hsh2, _ := root2.GetTableHash(ctx, tbl)

		if hsh1 != hsh2 {
			differing = append(differing, tbl)
		}
	}

	return differing
}

func intersect(sl1, sl2 []string) []string {
	sl1Members := make(map[string]struct{})

	for _, mem := range sl1 {
		sl1Members[mem] = struct{}{}
	}

	maxIntSize := math.MaxInt(len(sl1), len(sl2))

	intersection := make([]string, 0, maxIntSize)
	for _, mem := range sl2 {
		if _, ok := sl1Members[mem]; ok {
			intersection = append(intersection, mem)
		}
	}

	return intersection
}

func RootsWithTable(ctx context.Context, dEnv *env.DoltEnv, table string) (RootTypeSet, error) {
	roots, err := getRoots(ctx, dEnv, ActiveRoots...)

	if err != nil {
		return nil, err
	}

	rootsWithTable := make([]RootType, 0, len(roots))
	for rt, root := range roots {
		if root.HasTable(ctx, table) {
			rootsWithTable = append(rootsWithTable, rt)
		}
	}

	return NewRootTypeSet(rootsWithTable...), nil
}

func BranchOrTable(ctx context.Context, dEnv *env.DoltEnv, str string) (bool, RootTypeSet, error) {
	rootsWithTbl, err := RootsWithTable(ctx, dEnv, str)

	if err != nil {
		return false, nil, err
	}

	return dEnv.DoltDB.HasBranch(str), rootsWithTbl, nil
}

func MaybeGetCommit(dEnv *env.DoltEnv, str string) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(str, dEnv.RepoState.Branch)

	if err == nil {
		cm, err := dEnv.DoltDB.Resolve(cs)

		switch err {
		case nil:
			return cm, nil

		case doltdb.ErrHashNotFound, doltdb.ErrBranchNotFound:
			return nil, nil

		default:
			return nil, err
		}
	}

	return nil, nil
}
