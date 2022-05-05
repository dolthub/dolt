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
	"errors"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	tagMetaField      = "meta"
	tagCommitRefField = "ref"
	tagName           = "Tag"
)

var tagTemplate = types.MakeStructTemplate(tagName, []string{tagMetaField, tagCommitRefField})

// ref is a Ref<Commit>, but 'Commit' is not defined in this snippet.
// Tag refs are validated to point at Commits during write.
var valueTagType = nomdl.MustParseType(`Struct Tag {
        meta: Struct {},
        ref:  Ref<Value>,
}`)

// TagOptions is used to pass options into Tag.
type TagOptions struct {
	// Meta is a Struct that describes arbitrary metadata about this Tag,
	// e.g. a timestamp or descriptive text.
	Meta *TagMeta
}

// newTag serializes a tag pointing to |commitAddr| with the given |meta|,
// persists it, and returns its addr. Also returns a types.Ref to the tag, if
// the format for |db| is noms.
func newTag(ctx context.Context, db *database, commitAddr hash.Hash, meta *TagMeta) (hash.Hash, types.Ref, error) {
	if !db.Format().UsesFlatbuffers() {
		commitSt, err := db.ReadValue(ctx, commitAddr)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}
		iscommit, err := IsCommit(commitSt)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}
		if !iscommit {
			return hash.Hash{}, types.Ref{}, errors.New("newTag: commitAddr does not point to a commit.")
		}
		commitRef, err := types.NewRef(commitSt, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		var metaV types.Struct
		if meta != nil {
			var err error
			metaV, err = meta.toNomsStruct(commitRef.Format())
			if err != nil {
				return hash.Hash{}, types.Ref{}, err
			}
		} else {
			metaV = types.EmptyStruct(commitRef.Format())
		}
		tagSt, err := tagTemplate.NewStruct(metaV.Format(), []types.Value{metaV, commitRef})
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		tagRef, err := db.WriteValue(ctx, tagSt)
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		ref, err := types.ToRefOfValue(tagRef, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		return ref.TargetHash(), ref, nil
	} else {
		data := tag_flatbuffer(commitAddr, meta)
		r, err := db.WriteValue(ctx, types.SerialMessage(data))
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		ref, err := types.ToRefOfValue(r, db.Format())
		if err != nil {
			return hash.Hash{}, types.Ref{}, err
		}

		return ref.TargetHash(), ref, nil
	}
}

func tag_flatbuffer(commitAddr hash.Hash, meta *TagMeta) []byte {
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
	builder.FinishWithFileIdentifier(serial.TagEnd(builder), []byte(serial.TagFileID))
	return builder.FinishedBytes()
}

func IsTag(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); ok {
		return types.IsValueSubtypeOf(s.Format(), v, valueTagType)
	} else if sm, ok := v.(types.SerialMessage); ok {
		data := []byte(sm)
		return serial.GetFileID(data) == serial.TagFileID, nil
	}
	return false, nil
}

func makeTagStructType(metaType, refType *types.Type) (*types.Type, error) {
	return types.MakeStructType(tagName,
		types.StructField{
			Name: tagMetaField,
			Type: metaType,
		},
		types.StructField{
			Name: tagCommitRefField,
			Type: refType,
		},
	)
}
