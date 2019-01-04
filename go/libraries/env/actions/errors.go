package actions

import "strings"

type TblNotExist struct {
	Tables []string
}

func (tne TblNotExist) Error() string {
	return "error: the tables " + strings.Join(tne.Tables, ", ") + "do not exist"
}

func IsTblNotExist(err error) bool {
	_, ok := err.(TblNotExist)
	return ok
}

func GetTblNotExistTables(err error) []string {
	tne, ok := err.(TblNotExist)

	if !ok {
		panic("Must validate with IsTblNotExist before calling GetTblNotExistTables")
	}

	return tne.Tables
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
	NotStaged *TableDiffs
}

func (ns NothingStaged) Error() string {
	return "no changes added to commit"
}

func IsNothingStaged(err error) bool {
	_, ok := err.(NothingStaged)
	return ok
}

func NothingStagedDiffs(err error) *TableDiffs {
	ns, ok := err.(NothingStaged)

	if !ok {
		panic("Must validate with IsCheckoutWouldOverwrite before calling CheckoutWouldOverwriteTables")
	}

	return ns.NotStaged
}
