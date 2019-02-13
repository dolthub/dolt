package doltdb

import "errors"

var ErrInvBranchName = errors.New("not a valid user branch name")
var ErrInvTableName = errors.New("not a valid table name")
var ErrInvHash = errors.New("not a valid hash")
var ErrInvalidAnscestorSpec = errors.New("invalid anscestor spec")
var ErrInvalidBranchOrHash = errors.New("string is not a valid branch or hash")

var ErrFoundHashNotACommit = errors.New("the value retrieved for this hash is not a commit")

var ErrHashNotFound = errors.New("could not find a value for this hash")
var ErrBranchNotFound = errors.New("branch not found")
var ErrTableNotFound = errors.New("table not found")

var ErrNomsIO = errors.New("error reading from or writing to noms")

var ErrNoConflicts = errors.New("no conflicts")
var ErrUpToDate = errors.New("up to date")
var ErrIsAhead = errors.New("current commit is ahead of new commit")

func IsInvalidFormatErr(err error) bool {
	switch err {
	case ErrInvBranchName, ErrInvTableName, ErrInvHash, ErrInvalidAnscestorSpec, ErrInvalidBranchOrHash:
		return true
	default:
		return false
	}
}

func IsNotFoundErr(err error) bool {
	switch err {
	case ErrHashNotFound, ErrBranchNotFound, ErrTableNotFound:
		return true
	default:
		return false
	}
}

func IsNotACommit(err error) bool {
	switch err {
	case ErrHashNotFound, ErrBranchNotFound, ErrFoundHashNotACommit:
		return true
	default:
		return false
	}
}
