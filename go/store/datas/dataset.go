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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

type refnameAction byte

const (
	refnameOk        refnameAction = 0
	refnameEof       refnameAction = 1
	refnameDot       refnameAction = 2
	refnameLeftCurly refnameAction = 3
	refnameIllegal   refnameAction = 4
)

// ValidateDatasetId returns ErrInvalidDatasetID if the given dataset ID is invalid.
// See rules in |validateDatasetIdComponent|
// git additionally requires at least 2 path components to a ref, which we do not
func ValidateDatasetId(refname string) error {
	var componentCount int

	if len(refname) == 0 {
		return ErrInvalidDatasetID
	}

	if refname == "@" {
		// Refname is a single character '@'.
		return ErrInvalidDatasetID
	}

	if strings.HasSuffix(refname, "/") || strings.HasSuffix(refname, ".") {
		return ErrInvalidDatasetID
	}

	for len(refname) > 0 {
		componentLen, err := validateDatasetIdComponent(refname)
		if err != nil {
			return err
		}

		componentCount++

		// Next component
		refname = refname[componentLen:]
	}

	return nil
}

// How to handle various characters in refnames:
// 0: An acceptable character for refs
// 1: End-of-component ('/')
// 2: ., look for a preceding . to reject .. in refs
// 3: {, look for a preceding @ to reject @{ in refs
// 4: A bad character: ASCII control characters, and
//
//	":", "?", "[", "\", "^", "~", SP, or TAB
var refnameActions = [256]refnameAction{
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 2, 1,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 4,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 0, 4, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 4, 4,
}

// validateDatasetIdComponent returns an error if the dataset name component given is illegal.
// We use the same rules as git for each slash-separated component. Rules defined here:
// https://github.com/git/git/blob/master/refs.c
// Names must be ascii only. We reject the following in ref components:
// * - it begins with "."
// * - it has double dots ".."
// * - it has ASCII control characters
// * - it has ":", "?", "[", "\", "^", "~", "*", SP, or TAB anywhere
// * - it ends with a "/"
// * - it ends with ".lock"
// * - it contains a "@{" portion
func validateDatasetIdComponent(refname string) (int, error) {
	if refname[0] == '.' { // Component starts with '.'
		return -1, ErrInvalidDatasetID
	}

	var last rune
	numChars := 0

	for _, ch := range refname {
		if ch > unicode.MaxASCII {
			return -1, ErrInvalidDatasetID
		}

		numChars++

		switch refnameActions[ch] {
		case refnameOk:
		case refnameEof:
			if strings.HasSuffix(refname[:numChars-1], ".lock") {
				return -1, ErrInvalidDatasetID
			}
			return numChars, nil
		case refnameDot:
			if last == '.' { // Refname contains ..
				return -1, ErrInvalidDatasetID
			}
		case refnameLeftCurly:
			if last == '@' { // Refname contains @{
				return -1, ErrInvalidDatasetID
			}
		case refnameIllegal:
			return -1, ErrInvalidDatasetID
		default:
			panic("unrecognized case in refname")
		}

		last = ch
	}

	if strings.HasSuffix(refname[:numChars], ".lock") {
		return -1, ErrInvalidDatasetID
	}

	return numChars, nil
}

type WorkingSetHead struct {
	Meta        *WorkingSetMeta
	WorkingAddr hash.Hash
	StagedAddr  *hash.Hash
	MergeState  *MergeState
	RebaseState *RebaseState
}

type RebaseState struct {
	preRebaseWorkingAddr       *hash.Hash
	ontoCommitAddr             *hash.Hash
	branch                     string
	commitBecomesEmptyHandling uint8
	emptyCommitHandling        uint8
	lastAttemptedStep          float32
	rebasingStarted            bool
}

func (rs *RebaseState) PreRebaseWorkingAddr() hash.Hash {
	if rs.preRebaseWorkingAddr != nil {
		return *rs.preRebaseWorkingAddr
	} else {
		return hash.Hash{}
	}
}

func (rs *RebaseState) Branch(_ context.Context) string {
	return rs.branch
}

func (rs *RebaseState) OntoCommit(ctx context.Context, vr types.ValueReader) (*Commit, error) {
	if rs.ontoCommitAddr != nil {
		return LoadCommitAddr(ctx, vr, *rs.ontoCommitAddr)
	}
	return nil, nil
}

func (rs *RebaseState) LastAttemptedStep(_ context.Context) float32 {
	return rs.lastAttemptedStep
}

func (rs *RebaseState) RebasingStarted(_ context.Context) bool {
	return rs.rebasingStarted
}

func (rs *RebaseState) CommitBecomesEmptyHandling(_ context.Context) uint8 {
	return rs.commitBecomesEmptyHandling
}

func (rs *RebaseState) EmptyCommitHandling(_ context.Context) uint8 {
	return rs.emptyCommitHandling
}

type MergeState struct {
	preMergeWorkingAddr *hash.Hash
	fromCommitAddr      *hash.Hash
	fromCommitSpec      string
	unmergableTables    []string
	isCherryPick        bool

	nomsMergeStateRef *types.Ref
	nomsMergeState    *types.Struct
}

func (ms *MergeState) loadIfNeeded(ctx context.Context, vr types.ValueReader) error {
	if ms.nomsMergeState == nil {
		v, err := ms.nomsMergeStateRef.TargetValue(ctx, vr)
		if err != nil {
			return err
		}
		if v == nil {
			return errors.New("dangling reference to merge state")
		}
		st, ok := v.(types.Struct)
		if !ok {
			return fmt.Errorf("corrupted MergeState struct")
		}
		ms.nomsMergeState = &st
	}
	return nil
}

func (ms *MergeState) PreMergeWorkingAddr(ctx context.Context, vr types.ValueReader) (hash.Hash, error) {
	if ms.preMergeWorkingAddr != nil {
		return *ms.preMergeWorkingAddr, nil
	}
	if ms.nomsMergeState == nil {
		err := ms.loadIfNeeded(ctx, vr)
		if err != nil {
			return hash.Hash{}, err
		}
	}

	workingRootRef, ok, err := ms.nomsMergeState.MaybeGet(mergeStateWorkingPreMergeField)
	if err != nil {
		return hash.Hash{}, err
	}
	if !ok {
		return hash.Hash{}, fmt.Errorf("corrupted MergeState struct")
	}
	return workingRootRef.(types.Ref).TargetHash(), nil
}

func (ms *MergeState) FromCommit(ctx context.Context, vr types.ValueReader) (*Commit, error) {
	if ms.fromCommitAddr != nil {
		return LoadCommitAddr(ctx, vr, *ms.fromCommitAddr)
	}
	if ms.nomsMergeState == nil {
		err := ms.loadIfNeeded(ctx, vr)
		if err != nil {
			return nil, err
		}
	}

	commitV, ok, err := ms.nomsMergeState.MaybeGet(mergeStateCommitField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("corrupted MergeState struct")
	}

	return CommitFromValue(vr.Format(), commitV)
}

func (ms *MergeState) FromCommitSpec(ctx context.Context, vr types.ValueReader) (string, error) {
	if vr.Format().UsesFlatbuffers() {
		return ms.fromCommitSpec, nil
	}

	if ms.nomsMergeState == nil {
		err := ms.loadIfNeeded(ctx, vr)
		if err != nil {
			return "", err
		}
	}

	commitSpecStr, ok, err := ms.nomsMergeState.MaybeGet(mergeStateCommitSpecField)
	if err != nil {
		return "", err
	}
	if !ok {
		// Allow noms merge state to be backwards compatible with merge states
		// that previously did not have a commit spec string.
		return "", nil
	}

	return string(commitSpecStr.(types.String)), nil
}

func (ms *MergeState) IsCherryPick(_ context.Context, vr types.ValueReader) (bool, error) {
	if vr.Format().UsesFlatbuffers() {
		return ms.isCherryPick, nil
	}
	return false, nil
}

func (ms *MergeState) UnmergableTables(ctx context.Context, vr types.ValueReader) ([]string, error) {
	if vr.Format().UsesFlatbuffers() {
		return ms.unmergableTables, nil
	}
	return nil, nil
}

type dsHead interface {
	TypeName() string
	Addr() hash.Hash
	HeadTag() (*TagMeta, hash.Hash, error)
	HeadWorkingSet() (*WorkingSetHead, error)

	value() types.Value
}

type nomsHead struct {
	st   types.Struct
	addr hash.Hash
}

func (h nomsHead) TypeName() string {
	return h.st.Name()
}

func (h nomsHead) Addr() hash.Hash {
	return h.addr
}

func (h nomsHead) value() types.Value {
	return h.st
}

type serialTagHead struct {
	msg  *serial.Tag
	addr hash.Hash
}

func newSerialTagHead(bs []byte, addr hash.Hash) (serialTagHead, error) {
	tm, err := serial.TryGetRootAsTag(bs, serial.MessagePrefixSz)
	if err != nil {
		return serialTagHead{}, err
	}
	return serialTagHead{tm, addr}, nil
}

func (h serialTagHead) TypeName() string {
	return tagName
}

func (h serialTagHead) Addr() hash.Hash {
	return h.addr
}

func (h serialTagHead) value() types.Value {
	return types.SerialMessage(h.msg.Table().Bytes)
}

func (h serialTagHead) HeadTag() (*TagMeta, hash.Hash, error) {
	addr := hash.New(h.msg.CommitAddrBytes())
	meta := &TagMeta{
		Name:          string(h.msg.Name()),
		Email:         string(h.msg.Email()),
		Timestamp:     h.msg.TimestampMillis(),
		Description:   string(h.msg.Desc()),
		UserTimestamp: h.msg.UserTimestampMillis(),
	}
	return meta, addr, nil
}

func (h serialTagHead) HeadWorkingSet() (*WorkingSetHead, error) {
	return nil, errors.New("HeadWorkingSet called on tag")
}

type serialWorkingSetHead struct {
	msg  *serial.WorkingSet
	addr hash.Hash
}

func newSerialWorkingSetHead(bs []byte, addr hash.Hash) (serialWorkingSetHead, error) {
	fb, err := serial.TryGetRootAsWorkingSet(bs, serial.MessagePrefixSz)
	if err != nil {
		return serialWorkingSetHead{}, err
	}
	return serialWorkingSetHead{fb, addr}, nil
}

func (h serialWorkingSetHead) TypeName() string {
	return workingSetName
}

func (h serialWorkingSetHead) Addr() hash.Hash {
	return h.addr
}

func (h serialWorkingSetHead) value() types.Value {
	return types.SerialMessage(h.msg.Table().Bytes)
}

func (h serialWorkingSetHead) HeadTag() (*TagMeta, hash.Hash, error) {
	return nil, hash.Hash{}, errors.New("HeadTag called on working set")
}

func (h serialWorkingSetHead) HeadWorkingSet() (*WorkingSetHead, error) {
	var ret WorkingSetHead
	ret.Meta = &WorkingSetMeta{
		Name:        string(h.msg.Name()),
		Email:       string(h.msg.Email()),
		Timestamp:   h.msg.TimestampMillis(),
		Description: string(h.msg.Desc()),
	}
	ret.WorkingAddr = hash.New(h.msg.WorkingRootAddrBytes())
	if h.msg.StagedRootAddrLength() != 0 {
		ret.StagedAddr = new(hash.Hash)
		*ret.StagedAddr = hash.New(h.msg.StagedRootAddrBytes())
	}
	mergeState, err := h.msg.TryMergeState(nil)
	if err != nil {
		return nil, err
	}
	if mergeState != nil {
		ret.MergeState = &MergeState{
			preMergeWorkingAddr: new(hash.Hash),
			fromCommitAddr:      new(hash.Hash),
			fromCommitSpec:      string(mergeState.FromCommitSpecStr()),
		}
		*ret.MergeState.preMergeWorkingAddr = hash.New(mergeState.PreWorkingRootAddrBytes())
		*ret.MergeState.fromCommitAddr = hash.New(mergeState.FromCommitAddrBytes())
		ret.MergeState.unmergableTables = make([]string, mergeState.UnmergableTablesLength())
		for i := range ret.MergeState.unmergableTables {
			ret.MergeState.unmergableTables[i] = string(mergeState.UnmergableTables(i))
		}
		ret.MergeState.isCherryPick = mergeState.IsCherryPick()
	}

	rebaseState, err := h.msg.TryRebaseState(nil)
	if err != nil {
		return nil, err
	}
	if rebaseState != nil {
		ret.RebaseState = NewRebaseState(
			hash.New(rebaseState.PreWorkingRootAddrBytes()),
			hash.New(rebaseState.OntoCommitAddrBytes()),
			string(rebaseState.BranchBytes()),
			rebaseState.CommitBecomesEmptyHandling(),
			rebaseState.EmptyCommitHandling(),
			rebaseState.LastAttemptedStep(),
			rebaseState.RebasingStarted(),
		)
	}

	return &ret, nil
}

type serialCommitHead struct {
	msg  types.SerialMessage
	addr hash.Hash
}

func newSerialCommitHead(sm types.SerialMessage, addr hash.Hash) serialCommitHead {
	return serialCommitHead{sm, addr}
}

func (h serialCommitHead) TypeName() string {
	return commitName
}

func (h serialCommitHead) Addr() hash.Hash {
	return h.addr
}

func (h serialCommitHead) value() types.Value {
	return h.msg
}

func (h serialCommitHead) HeadTag() (*TagMeta, hash.Hash, error) {
	return nil, hash.Hash{}, errors.New("HeadTag called on commit")
}

func (h serialCommitHead) HeadWorkingSet() (*WorkingSetHead, error) {
	return nil, errors.New("HeadWorkingSet called on commit")
}

type serialStashListHead struct {
	msg  types.SerialMessage
	addr hash.Hash
}

func newSerialStashListHead(sm types.SerialMessage, addr hash.Hash) serialStashListHead {
	return serialStashListHead{sm, addr}
}

func (h serialStashListHead) TypeName() string {
	return stashListName
}

func (h serialStashListHead) Addr() hash.Hash {
	return h.addr
}

func (h serialStashListHead) value() types.Value {
	return h.msg
}

func (h serialStashListHead) HeadTag() (*TagMeta, hash.Hash, error) {
	return nil, hash.Hash{}, errors.New("HeadTag called on stash list")
}

func (h serialStashListHead) HeadWorkingSet() (*WorkingSetHead, error) {
	return nil, errors.New("HeadWorkingSet called on stash list")
}

func newStatisticHead(sm types.SerialMessage, addr hash.Hash) serialStashListHead {
	return serialStashListHead{sm, addr}
}

type statisticsHead struct {
	msg  types.SerialMessage
	addr hash.Hash
}

var _ dsHead = statisticsHead{}

// TypeName implements dsHead
func (s statisticsHead) TypeName() string {
	return "Statistics"
}

// Addr implements dsHead
func (s statisticsHead) Addr() hash.Hash {
	return s.addr
}

// HeadTag implements dsHead
func (s statisticsHead) HeadTag() (*TagMeta, hash.Hash, error) {
	return nil, hash.Hash{}, errors.New("HeadTag called on statistic")
}

// HeadWorkingSet implements dsHead
func (s statisticsHead) HeadWorkingSet() (*WorkingSetHead, error) {
	return nil, errors.New("HeadWorkingSet called on statistic")
}

// value implements dsHead
func (s statisticsHead) value() types.Value {
	return s.msg
}

// Dataset is a named value within a Database. Different head values may be stored in a dataset. Most commonly, this is
// a commit, but other values are also supported in some cases.
type Dataset struct {
	db   *database
	id   string
	head dsHead
}

// LoadRootNomsValueFromRootIshAddr returns the types.Value encoded root value
// from a "root-ish" |addr|. The |addr| might be the |addr| of a working set or
// the |addr| of a commit.
func LoadRootNomsValueFromRootIshAddr(ctx context.Context, vr types.ValueReader, addr hash.Hash) (types.Value, error) {
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}
	h, err := newHead(ctx, v, addr)
	if err != nil {
		return nil, err
	}

	switch h.TypeName() {
	case workingSetName:
		ws, err := h.HeadWorkingSet()
		if err != nil {
			return nil, err
		}
		return vr.ReadValue(ctx, ws.WorkingAddr)
	case commitName:
		dsCm, err := LoadCommitAddr(ctx, vr, h.Addr())
		if err != nil {
			return nil, err
		}
		return GetCommittedValue(ctx, vr, dsCm.NomsValue())
	default:
		panic(fmt.Sprintf("loading root value from dsHead type %s not implemented", h.TypeName()))
	}
}

func newHead(ctx context.Context, head types.Value, addr hash.Hash) (dsHead, error) {
	if head == nil {
		return nil, nil
	}

	if sm, ok := head.(types.SerialMessage); ok {
		data := []byte(sm)
		switch serial.GetFileID(data) {
		case serial.TagFileID:
			return newSerialTagHead(data, addr)
		case serial.WorkingSetFileID:
			return newSerialWorkingSetHead(data, addr)
		case serial.CommitFileID:
			return newSerialCommitHead(sm, addr), nil
		case serial.StashListFileID:
			return newSerialStashListHead(sm, addr), nil
		case serial.StatisticFileID:
			return newStatisticHead(sm, addr), nil
		}
	}

	matched, err := IsCommit(head)
	if err != nil {
		return nil, err
	}
	if !matched {
		matched, err = IsTag(ctx, head)
		if err != nil {
			return nil, err
		}
	}
	if !matched {
		matched, err = IsWorkingSet(head)
		if err != nil {
			return nil, err
		}
	}
	if !matched {
		return nil, fmt.Errorf("database: fetched head at %v but it was not a commit, tag or working set.", addr)
	}

	return nomsHead{head.(types.Struct), addr}, nil
}

func newDataset(ctx context.Context, db *database, id string, head types.Value, addr hash.Hash) (Dataset, error) {
	h, err := newHead(ctx, head, addr)
	if err != nil {
		return Dataset{}, err
	}
	return Dataset{db, id, h}, nil
}

// Database returns the Database object in which this Dataset is stored.
// WARNING: This method is under consideration for deprecation.
func (ds Dataset) Database() Database {
	return ds.db
}

// ID returns the name of this Dataset.
func (ds Dataset) ID() string {
	return ds.id
}

// MaybeHead returns the current Head Commit of this Dataset, which contains
// the current root of the Dataset's value tree, if available. If not, it
// returns a new Commit and 'false'.
func (ds Dataset) MaybeHead() (types.Value, bool) {
	if ds.head == nil {
		return types.Struct{}, false
	}
	if nh, ok := ds.head.(nomsHead); ok {
		return nh.st, true
	} else if sch, ok := ds.head.(serialCommitHead); ok {
		return sch.msg, true
	} else if slh, ok := ds.head.(serialStashListHead); ok {
		return slh.msg, true
	}
	panic("unexpected ds.head type for MaybeHead call")
}

// MaybeHeadRef returns the Ref of the current Head Commit of this Dataset,
// which contains the current root of the Dataset's value tree, if available.
// If not, it returns an empty Ref and 'false'.
func (ds Dataset) MaybeHeadRef() (types.Ref, bool, error) {
	st, ok := ds.MaybeHead()
	if !ok {
		return types.Ref{}, false, nil
	}
	ref, err := types.NewRef(st, ds.db.Format())
	if err != nil {
		return types.Ref{}, false, err
	}
	return ref, true, nil
}

func (ds Dataset) MaybeHeadAddr() (hash.Hash, bool) {
	if ds.head == nil {
		return hash.Hash{}, false
	}
	return ds.head.Addr(), true
}

func (ds Dataset) MaybeHeight() (uint64, bool, error) {
	r, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return r.Height(), true, nil
}

func (ds Dataset) IsTag() bool {
	return ds.head != nil && ds.head.TypeName() == tagName
}

func (ds Dataset) IsWorkingSet() bool {
	return ds.head != nil && ds.head.TypeName() == workingSetName
}

func (ds Dataset) HeadTag() (*TagMeta, hash.Hash, error) {
	if ds.head == nil {
		return nil, hash.Hash{}, errors.New("no head value for HeadTag call")
	}
	if !ds.IsTag() {
		return nil, hash.Hash{}, errors.New("HeadTag call on non-tag head")
	}
	return ds.head.HeadTag()
}

func (ds Dataset) HeadWorkingSet() (*WorkingSetHead, error) {
	if ds.head == nil {
		return nil, errors.New("no head value for HeadWorkingSet call")
	}
	if !ds.IsWorkingSet() {
		return nil, errors.New("HeadWorkingSet call on non-working set head")
	}
	return ds.head.HeadWorkingSet()
}

func (h nomsHead) HeadTag() (*TagMeta, hash.Hash, error) {
	metast, ok, err := h.st.MaybeGet(tagMetaField)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	if !ok {
		return nil, hash.Hash{}, errors.New("no meta field in tag struct head")
	}
	meta, err := tagMetaFromNomsSt(metast.(types.Struct))
	if err != nil {
		return nil, hash.Hash{}, err
	}

	commitRef, ok, err := h.st.MaybeGet(tagCommitRefField)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	if !ok {
		return nil, hash.Hash{}, errors.New("tag struct does not have field commit field")
	}
	commitaddr := commitRef.(types.Ref).TargetHash()

	return meta, commitaddr, nil
}

func (h nomsHead) HeadWorkingSet() (*WorkingSetHead, error) {
	st := h.st

	var ret WorkingSetHead

	meta, err := workingSetMetaFromWorkingSetSt(st)
	if err != nil {
		return nil, err
	}
	ret.Meta = meta

	workingRootRef, ok, err := st.MaybeGet(workingRootRefField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("workingset struct does not have field %s", workingRootRefField)
	}
	ret.WorkingAddr = workingRootRef.(types.Ref).TargetHash()

	stagedRootRef, ok, err := st.MaybeGet(stagedRootRefField)
	if err != nil {
		return nil, err
	}
	if ok {
		ret.StagedAddr = new(hash.Hash)
		*ret.StagedAddr = stagedRootRef.(types.Ref).TargetHash()
	}

	mergeStateRef, ok, err := st.MaybeGet(mergeStateField)
	if err != nil {
		return nil, err
	}
	if ok {
		r := mergeStateRef.(types.Ref)
		ret.MergeState = &MergeState{
			nomsMergeStateRef: &r,
		}
	}

	return &ret, nil
}

// HasHead() returns 'true' if this dataset has a Head Commit, false otherwise.
func (ds Dataset) HasHead() bool {
	return ds.head != nil
}

// MaybeHeadValue returns the Value field of the current head Commit, if
// available. If not it returns nil and 'false'.
func (ds Dataset) MaybeHeadValue() (types.Value, bool, error) {
	if c, ok := ds.MaybeHead(); ok {
		v, err := GetCommittedValue(context.TODO(), ds.db, c)
		if err != nil {
			return nil, false, err
		}
		return v, v != nil, nil
	}
	return nil, false, nil
}

func NewHeadlessDataset(db Database, id string) Dataset {
	return Dataset{
		id:   id,
		head: nil,
		db:   db.(*database),
	}
}
