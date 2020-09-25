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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
)

type tblErrorType string

const (
	tblErrInvalid        tblErrorType = "invalid"
	tblErrTypeNotExist   tblErrorType = "do not exist"
	tblErrTypeInConflict tblErrorType = "in conflict"
)

type TblError struct {
	tables     []string
	tblErrType tblErrorType
}

func NewTblNotExistError(tbls []string) TblError {
	return TblError{tbls, tblErrTypeNotExist}
}

func NewTblInConflictError(tbls []string) TblError {
	return TblError{tbls, tblErrTypeInConflict}
}

func (te TblError) Error() string {
	return "error: the tables " + strings.Join(te.tables, ", ") + string(te.tblErrType)
}

func getTblErrType(err error) tblErrorType {
	te, ok := err.(TblError)

	if ok {
		return te.tblErrType
	}

	return tblErrInvalid
}

func IsTblError(err error) bool {
	return getTblErrType(err) != tblErrInvalid
}

func IsTblNotExist(err error) bool {
	return getTblErrType(err) == tblErrTypeNotExist
}

func IsTblInConflict(err error) bool {
	return getTblErrType(err) == tblErrTypeInConflict
}

func GetTablesForError(err error) []string {
	te, ok := err.(TblError)

	if !ok {
		panic("Must validate with IsTblError or more specific methods before calling GetTablesForError")
	}

	return te.tables
}

type RootType int

const (
	WorkingRoot RootType = iota
	StagedRoot
	CommitRoot
	HeadRoot
	InvalidRoot
)

var ActiveRoots = []RootType{WorkingRoot, StagedRoot, HeadRoot}

func (rt RootType) String() string {
	switch rt {
	case WorkingRoot:
		return "working root"
	case StagedRoot:
		return "staged root"
	case CommitRoot:
		return "root value for commit"
	case HeadRoot:
		return "HEAD commit root value"
	}

	return "unknown"
}

type RootTypeSet map[RootType]struct{}

func NewRootTypeSet(rts ...RootType) RootTypeSet {
	mp := make(map[RootType]struct{})

	for _, rt := range rts {
		mp[rt] = struct{}{}
	}

	return RootTypeSet(mp)
}

func (rts RootTypeSet) Contains(rt RootType) bool {
	_, ok := rts[rt]
	return ok
}

func (rts RootTypeSet) First(rtList []RootType) RootType {
	for _, rt := range rtList {
		if _, ok := rts[rt]; ok {
			return rt
		}
	}

	return InvalidRoot
}

func (rts RootTypeSet) IsEmpty() bool {
	return len(rts) == 0
}

type RootValueUnreadable struct {
	rootType RootType
	Cause    error
}

func (rvu RootValueUnreadable) Error() string {
	return "error: Unable to read " + rvu.rootType.String()
}

func IsRootValUnreachable(err error) bool {
	_, ok := err.(RootValueUnreadable)
	return ok
}

func GetUnreachableRootType(err error) RootType {
	rvu, ok := err.(RootValueUnreadable)

	if !ok {
		panic("Must validate with IsRootValUnreachable before calling GetUnreachableRootType")
	}

	return rvu.rootType
}

func GetUnreachableRootCause(err error) error {
	rvu, ok := err.(RootValueUnreadable)

	if !ok {
		panic("Must validate with IsRootValUnreachable before calling GetUnreachableRootCause")
	}

	return rvu.Cause
}

type CheckoutWouldOverwrite struct {
	tables []string
}

func (cwo CheckoutWouldOverwrite) Error() string {
	return "local changes would be overwritten by overwrite"
}

func IsCheckoutWouldOverwrite(err error) bool {
	_, ok := err.(CheckoutWouldOverwrite)
	return ok
}

func CheckoutWouldOverwriteTables(err error) []string {
	cwo, ok := err.(CheckoutWouldOverwrite)

	if !ok {
		panic("Must validate with IsCheckoutWouldOverwrite before calling CheckoutWouldOverwriteTables")
	}

	return cwo.tables
}

type NothingStaged struct {
	NotStagedTbls []diff.TableDelta
	NotStagedDocs *diff.DocDiffs
}

func (ns NothingStaged) Error() string {
	return "no changes added to commit"
}

func IsNothingStaged(err error) bool {
	_, ok := err.(NothingStaged)
	return ok
}

func NothingStagedTblDiffs(err error) []diff.TableDelta {
	ns, ok := err.(NothingStaged)

	if !ok {
		panic("Must validate with IsCheckoutWouldOverwrite before calling CheckoutWouldOverwriteTables")
	}

	return ns.NotStagedTbls
}

func NothingStagedDocsDiffs(err error) *diff.DocDiffs {
	ns, ok := err.(NothingStaged)

	if !ok {
		panic("Must validate with IsCheckoutWouldOverwrite before calling CheckoutWouldOverwriteTables")
	}

	return ns.NotStagedDocs
}
