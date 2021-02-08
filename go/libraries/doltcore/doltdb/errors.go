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

package doltdb

import (
	"errors"
	"fmt"
)

var ErrInvBranchName = errors.New("not a valid user branch name")
var ErrInvWorkspaceName = errors.New("not a valid user workspace name")
var ErrInvTagName = errors.New("not a valid user tag name")
var ErrInvTableName = errors.New("not a valid table name")
var ErrInvHash = errors.New("not a valid hash")
var ErrInvalidAncestorSpec = errors.New("invalid ancestor spec")
var ErrInvalidBranchOrHash = errors.New("string is not a valid branch or hash")
var ErrInvalidHash = errors.New("string is not a valid hash")

var ErrFoundHashNotACommit = errors.New("the value retrieved for this hash is not a commit")

var ErrHashNotFound = errors.New("could not find a value for this hash")
var ErrBranchNotFound = errors.New("branch not found")
var ErrTagNotFound = errors.New("tag not found")
var ErrWorkspaceNotFound = errors.New("workspace not found")
var ErrTableNotFound = errors.New("table not found")
var ErrTableExists = errors.New("table already exists")
var ErrAlreadyOnBranch = errors.New("Already on branch")
var ErrAlreadyOnWorkspace = errors.New("Already on workspace")

var ErrNomsIO = errors.New("error reading from or writing to noms")

var ErrNoConflicts = errors.New("no conflicts")
var ErrUpToDate = errors.New("up to date")
var ErrIsAhead = errors.New("current fast forward from a to b. a is ahead of b already")
var ErrIsBehind = errors.New("cannot reverse from b to a. b is a is behind a already")

type ErrClientOutOfDate struct {
	RepoVer   FeatureVersion
	ClientVer FeatureVersion
}

func (e ErrClientOutOfDate) Error() string {
	return fmt.Sprintf(`client (version: %d) is out of date and must upgrade to read this repo (version: %d).
	visit https://github.com/dolthub/dolt/releases/latest/`, e.ClientVer, e.RepoVer)
}

func IsInvalidFormatErr(err error) bool {
	switch err {
	case ErrInvBranchName, ErrInvTableName, ErrInvHash, ErrInvalidAncestorSpec, ErrInvalidBranchOrHash:
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
