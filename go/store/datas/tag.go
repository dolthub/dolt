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

	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	TagMetaField      = "meta"
	TagCommitRefField = "ref"
	TagName           = "Tag"
)

var tagTemplate = types.MakeStructTemplate(TagName, []string{TagMetaField, TagCommitRefField})

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

// newTag creates a new tag object.
//
// A tag has the following type:
//
// ```
// struct Tag {
//   meta: M,
//   commitRef: T,
// }
// ```
// where M is a struct type and R is a ref type.
func newTag(_ context.Context, commitRef types.Ref, meta *TagMeta) (types.Struct, error) {
	var metaV types.Struct
	if meta != nil {
		var err error
		metaV, err = meta.toNomsStruct(commitRef.Format())
		if err != nil {
			return types.Struct{}, err
		}
	} else {
		metaV = types.EmptyStruct(commitRef.Format())
	}
	return tagTemplate.NewStruct(metaV.Format(), []types.Value{metaV, commitRef})
}

func IsTag(v types.Value) (bool, error) {
	if s, ok := v.(types.Struct); !ok {
		return false, nil
	} else {
		return types.IsValueSubtypeOf(s.Format(), v, valueTagType)
	}
}

func makeTagStructType(metaType, refType *types.Type) (*types.Type, error) {
	return types.MakeStructType(TagName,
		types.StructField{
			Name: TagMetaField,
			Type: metaType,
		},
		types.StructField{
			Name: TagCommitRefField,
			Type: refType,
		},
	)
}
