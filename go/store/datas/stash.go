// Copyright 2023 Dolthub, Inc.
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

package datas

import (
	"context"
	"errors"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	stashListName = "StashList"
)

func newStashForValue(ctx context.Context, db *database, stashRef types.Ref, headAddr hash.Hash, meta *StashMeta) (hash.Hash, types.Ref, error) {
	if db.Format().UsesFlatbuffers() {
		headCommit, err := db.ReadValue(ctx, headAddr)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		isCommit, err := IsCommit(headCommit)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}
		if !isCommit {
			return hash.Hash{}, types.Ref{}, errors.New("newStash: headAddr does not point to a commit")
		}

		headRef, err := types.NewRef(headCommit, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		data := stash_flatbuffer(stashRef.TargetHash(), headRef.TargetHash(), meta)
		r, err := db.WriteValue(ctx, types.SerialMessage(data))
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		ref, err := types.ToRefOfValue(r, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		return ref.TargetHash(), ref, nil
	} else {
		return hash.Hash{}, types.Ref{}, errors.New("newStash: stash is not supported for old storage format")
	}
}

func stash_flatbuffer(stash, head hash.Hash, meta *StashMeta) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	stashOff := builder.CreateByteVector(stash[:])
	headOff := builder.CreateByteVector(head[:])
	branchNameOff := builder.CreateString(meta.BranchName)
	descOff := builder.CreateString(meta.Description)

	serial.StashStart(builder)
	serial.StashAddStashRootAddr(builder, stashOff)
	serial.StashAddHeadCommitAddr(builder, headOff)
	serial.StashAddBranchName(builder, branchNameOff)
	serial.StashAddDesc(builder, descOff)

	return serial.FinishMessage(builder, serial.StashEnd(builder), []byte(serial.StashFileID))
}

func IsStashList(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); ok {
		// TODO: do we need this check, as stash is not supported for old format
		return s.Name() == stashListName, nil
	} else if sm, ok := v.(types.SerialMessage); ok {
		return serial.GetFileID(sm) == serial.StashListFileID, nil
	} else {
		return false, nil
	}
}

func GetStashData(val types.Value) (hash.Hash, hash.Hash, *StashMeta, error) {
	bs := []byte(val.(types.SerialMessage))
	var msg serial.Stash
	err := serial.InitStashRoot(&msg, bs, serial.MessagePrefixSz)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, nil, err
	}

	meta := NewStashMeta(string(msg.BranchName()), string(msg.Desc()))
	sra := hash.New(msg.StashRootAddrBytes())
	hca := hash.New(msg.HeadCommitAddrBytes())

	return sra, hca, meta, err
}

const (
	stashMetaBranchNameKey = "branch_name"
	stashMetaDescKey       = "desc"

	stashMetaStName = "metadata"
)

// StashMeta contains all the metadata that is associated with a stash within a data repo.
type StashMeta struct {
	BranchName  string
	Description string
}

// NewStashMeta returns StashMeta that can be used to create a stash.
func NewStashMeta(name, desc string) *StashMeta {
	bn := strings.TrimSpace(name)
	d := strings.TrimSpace(desc)

	return &StashMeta{bn, d}
}
