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

package merge

import (
	"context"
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func NextConstraintViolation(ctx context.Context, itr prolly.ArtifactIter, kd, vd val.TupleDesc, ns tree.NodeStore) (violationType uint64, key sql.Row, value sql.Row, err error) {
	art, err := itr.Next(ctx)
	if err != nil {
		return
	}

	key = make(sql.UntypedSqlRow, kd.Count())
	var v interface{}
	for i := 0; i < kd.Count(); i++ {
		v, err = tree.GetField(ctx, kd, i, art.SourceKey, ns)
		if err != nil {
			return
		}
		key.SetValue(i, v)
	}

	var meta prolly.ConstraintViolationMeta
	err = json.Unmarshal(art.Metadata, &meta)
	if err != nil {
		return
	}

	value = make(sql.UntypedSqlRow, vd.Count())
	for i := 0; i < vd.Count(); i++ {
		v, err = tree.GetField(ctx, vd, i, meta.Value, ns)
		if err != nil {
			return
		}
		value.SetValue(i, v)
	}

	return MapCVType(art.ArtType), key, value, nil
}

func MapCVType(artifactType prolly.ArtifactType) (outType uint64) {
	switch artifactType {
	case prolly.ArtifactTypeForeignKeyViol:
		outType = uint64(CvType_ForeignKey)
	case prolly.ArtifactTypeUniqueKeyViol:
		outType = uint64(CvType_UniqueIndex)
	case prolly.ArtifactTypeChkConsViol:
		outType = uint64(CvType_CheckConstraint)
	case prolly.ArtifactTypeNullViol:
		outType = uint64(CvType_NotNull)
	default:
		panic("unhandled cv type")
	}
	return
}

func UnmapCVType(in CvType) (out prolly.ArtifactType) {
	switch in {
	case CvType_ForeignKey:
		out = prolly.ArtifactTypeForeignKeyViol
	case CvType_UniqueIndex:
		out = prolly.ArtifactTypeUniqueKeyViol
	case CvType_CheckConstraint:
		out = prolly.ArtifactTypeChkConsViol
	case CvType_NotNull:
		out = prolly.ArtifactTypeNullViol
	default:
		panic("unhandled cv type")
	}
	return
}
