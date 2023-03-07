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

// NewStash creates a new stash object.
func NewStash(ctx context.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, stashRef types.Ref, headAddr hash.Hash, meta *StashMeta) (hash.Hash, types.Ref, error) {
	if nbf.UsesFlatbuffers() {
		headCommit, err := vrw.ReadValue(ctx, headAddr)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		isCommit, err := IsCommit(ctx, headCommit)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}
		if !isCommit {
			return hash.Hash{}, types.Ref{}, errors.New("newStash: headAddr does not point to a commit")
		}

		headRef, err := types.NewRef(headCommit, nbf)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		data := stash_flatbuffer(stashRef.TargetHash(), headRef.TargetHash(), meta)
		r, err := vrw.WriteValue(ctx, types.SerialMessage(data))
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		ref, err := types.ToRefOfValue(r, nbf)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		return ref.TargetHash(), ref, nil
	} else {
		return hash.Hash{}, types.Ref{}, errors.New("newStash: stash is not supported for old storage format")
	}
}

// GetStashData takes types.Value, which should be of type types.SerialMessage as stash is supported only for new format.
// This function returns stashRoot address hash, head commit address hash and stash meta, which contains branch name
// that stash was made on and head commit meta description.
func GetStashData(val types.Value) (hash.Hash, hash.Hash, *StashMeta, error) {
	bs := []byte(val.(types.SerialMessage))
	var msg serial.Stash
	err := serial.InitStashRoot(&msg, bs, serial.MessagePrefixSz)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, nil, err
	}

	meta := NewStashMeta(string(msg.BranchName()), string(msg.Desc()))
	stashRootAddr := hash.New(msg.StashRootAddrBytes())
	headCommitAddr := hash.New(msg.HeadCommitAddrBytes())

	return stashRootAddr, headCommitAddr, meta, err
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
