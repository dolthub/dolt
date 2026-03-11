// Copyright 2020 Dolthub, Inc.
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

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tagMetaField      = "meta"
	tagCommitRefField = "ref"
	tagName           = "Tag"
)

// TagOptions is used to pass options into Tag.
type TagOptions struct {
	// Meta is a Struct that describes arbitrary metadata about this Tag,
	// e.g. a timestamp or descriptive text.
	Meta *TagMeta
}

// newTag serializes a tag pointing to |commitAddr| with the given |meta|,
// persists it, and returns its addr.
func newTag(ctx context.Context, db *database, commitAddr hash.Hash, meta *TagMeta) (hash.Hash, error) {
	data := tagSerialMessage(commitAddr, meta)
	r, err := db.WriteValue(ctx, types.SerialMessage(data))
	if err != nil {
		return hash.Hash{}, err
	}

	ref, err := types.ToRefOfValue(r, db.Format())
	if err != nil {
		return hash.Hash{}, err
	}

	return ref.TargetHash(), nil
}

func tagSerialMessage(commitAddr hash.Hash, meta *TagMeta) serial.Message {
	builder := flatbuffers.NewBuilder(1024)
	addroff := builder.CreateByteVector(commitAddr[:])
	var nameOff, emailOff, descOff flatbuffers.UOffsetT
	if meta != nil {
		nameOff = builder.CreateString(meta.Name)
		emailOff = builder.CreateString(meta.Email)
		descOff = builder.CreateString(meta.Description)
	}
	serial.TagStart(builder)
	serial.TagAddCommitAddr(builder, addroff)
	if meta != nil {
		serial.TagAddName(builder, nameOff)
		serial.TagAddEmail(builder, emailOff)
		serial.TagAddDesc(builder, descOff)
		serial.TagAddTimestampMillis(builder, meta.Timestamp)
		serial.TagAddUserTimestampMillis(builder, meta.UserTimestamp)
	}
	return serial.FinishMessage(builder, serial.TagEnd(builder), []byte(serial.TagFileID))
}

func IsTag(ctx context.Context, v types.Value) (bool, error) {
	if sm, ok := v.(types.SerialMessage); ok {
		data := []byte(sm)
		return serial.GetFileID(data) == serial.TagFileID, nil
	}
	return false, nil
}
