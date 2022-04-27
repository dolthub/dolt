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
	"regexp"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// DatasetRe is a regexp that matches a legal Dataset name anywhere within the
// target string.
var DatasetRe = regexp.MustCompile(`[a-zA-Z0-9\-_/]+`)

// DatasetFullRe is a regexp that matches a only a target string that is
// entirely legal Dataset name.
var DatasetFullRe = regexp.MustCompile("^" + DatasetRe.String() + "$")

type WorkingSetHead struct {
	Meta        *WorkingSetMeta
	WorkingAddr hash.Hash
	StagedAddr  *hash.Hash
	MergeState  *MergeState
}

type MergeState struct {
	preMergeWorkingAddr *hash.Hash
	fromCommitAddr      *hash.Hash

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

	return commitFromValue(vr.Format(), commitV)
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

func newSerialTagHead(bs []byte, addr hash.Hash) serialTagHead {
	return serialTagHead{serial.GetRootAsTag(bs, 0), addr}
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

func newSerialWorkingSetHead(bs []byte, addr hash.Hash) serialWorkingSetHead {
	return serialWorkingSetHead{serial.GetRootAsWorkingSet(bs, 0), addr}
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
	mergeState := h.msg.MergeState(nil)
	if mergeState != nil {
		ret.MergeState = &MergeState{
			preMergeWorkingAddr: new(hash.Hash),
			fromCommitAddr:      new(hash.Hash),
		}
		*ret.MergeState.preMergeWorkingAddr = hash.New(mergeState.PreWorkingRootAddrBytes())
		*ret.MergeState.fromCommitAddr = hash.New(mergeState.FromCommitAddrBytes())
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

// Dataset is a named value within a Database. Different head values may be stored in a dataset. Most commonly, this is
// a commit, but other values are also supported in some cases.
type Dataset struct {
	db   *database
	id   string
	head dsHead
}

func newHead(head types.Value, addr hash.Hash) (dsHead, error) {
	if head == nil {
		return nil, nil
	}

	if sm, ok := head.(types.SerialMessage); ok {
		data := []byte(sm)
		if serial.GetFileID(data) == serial.TagFileID {
			return newSerialTagHead(data, addr), nil
		}
		if serial.GetFileID(data) == serial.WorkingSetFileID {
			return newSerialWorkingSetHead(data, addr), nil
		}
		if serial.GetFileID(data) == serial.CommitFileID {
			return newSerialCommitHead(sm, addr), nil
		}
	}

	matched, err := IsCommit(head)
	if err != nil {
		return nil, err
	}
	if !matched {
		matched, err = IsTag(head)
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
		return nil, fmt.Errorf("database: fetched head at %v by it was not a commit, tag or working set.", addr)
	}

	return nomsHead{head.(types.Struct), addr}, nil
}

func newDataset(db *database, id string, head types.Value, addr hash.Hash) (Dataset, error) {
	h, err := newHead(head, addr)
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

func IsValidDatasetName(name string) bool {
	return DatasetFullRe.MatchString(name)
}
