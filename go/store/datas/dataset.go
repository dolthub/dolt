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
	"errors"
	"fmt"
	"regexp"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
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
	Meta           *WorkingSetMeta
	WorkingAddr    hash.Hash
	StagedAddr     *hash.Hash
	MergeStateAddr *hash.Hash
}

type dsHead interface {
	TypeName() string
	Addr() hash.Hash
	HeadTag() (*TagMeta, hash.Hash, error)
	HeadWorkingSet() (*WorkingSetHead, error)
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

type serialTagHead struct {
	msg  *serial.Tag
	addr hash.Hash
}

func newSerialTagHead(bs []byte, addr hash.Hash) serialTagHead {
	return serialTagHead{serial.GetRootAsTag(bs, 0), addr}
}

func (h serialTagHead) TypeName() string {
	return TagName
}

func (h serialTagHead) Addr() hash.Hash {
	return h.addr
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

// Dataset is a named value within a Database. Different head values may be stored in a dataset. Most commonly, this is
// a commit, but other values are also supported in some cases.
type Dataset struct {
	db   *database
	id   string
	head dsHead
}

func newHead(db *database, c chunks.Chunk) (dsHead, error) {
	if c.IsEmpty() {
		return nil, nil
	}

	if serial.GetFileID(c.Data()) == serial.TagFileID {
		return newSerialTagHead(c.Data(), c.Hash()), nil
	}

	head, err := types.DecodeValue(c, db)
	if err != nil {
		return nil, err
	}
	matched := false

	matched, err = IsCommit(head)
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
		return nil, fmt.Errorf("database: fetched head at %v by it was not a commit, tag or working set.", c.Hash())
	}
	return nomsHead{head.(types.Struct), c.Hash()}, nil
}

func newDataset(db *database, id string, headChunk chunks.Chunk) (Dataset, error) {
	h, err := newHead(db, headChunk)
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
func (ds Dataset) MaybeHead() (types.Struct, bool) {
	if ds.head == nil {
		return types.Struct{}, false
	}
	return ds.head.(nomsHead).st, true
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

func (ds Dataset) IsTag() bool {
	return ds.head != nil && ds.head.TypeName() == TagName
}

func (ds Dataset) IsWorkingSet() bool {
	return ds.head != nil && ds.head.TypeName() == WorkingSetName
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
	metast, ok, err := h.st.MaybeGet(TagMetaField)
	if err != nil {
		return nil, hash.Hash{}, err
	}
	if !ok {
		return nil, hash.Hash{}, errors.New("no meta field in tag struct head")
	}
	meta, err := TagMetaFromNomsSt(metast.(types.Struct))
	if err != nil {
		return nil, hash.Hash{}, err
	}

	commitRef, ok, err := h.st.MaybeGet(TagCommitRefField)
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

	meta, err := WorkingSetMetaFromWorkingSetSt(st)
	if err != nil {
		return nil, err
	}
	ret.Meta = meta

	workingRootRef, ok, err := st.MaybeGet(WorkingRootRefField)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("workingset struct does not have field %s", WorkingRootRefField)
	}
	ret.WorkingAddr = workingRootRef.(types.Ref).TargetHash()

	stagedRootRef, ok, err := st.MaybeGet(StagedRootRefField)
	if err != nil {
		return nil, err
	}
	if ok {
		ret.StagedAddr = new(hash.Hash)
		*ret.StagedAddr = stagedRootRef.(types.Ref).TargetHash()
	}

	mergeStateRef, ok, err := st.MaybeGet(MergeStateField)
	if err != nil {
		return nil, err
	}
	if ok {
		ret.MergeStateAddr = new(hash.Hash)
		*ret.MergeStateAddr = mergeStateRef.(types.Ref).TargetHash()
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
		return c.MaybeGet(ValueField)
	}
	return nil, false, nil
}

func IsValidDatasetName(name string) bool {
	return DatasetFullRe.MatchString(name)
}
