// Copyright 2021 Dolthub, Inc.
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

package geometry

/*import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

var ErrUnexpectedGeometryTypeIn = errors.New("unexpected type during Geometry marshalling")
var ErrUnexpectedGeometryTypeOut = errors.New("unexpected type during Geometry unmarshalling")

const (
	GeometryNull = "null"
)

// NomsGeometry is a type alias for types.Geometry. The alias allows MySQL-specific
// logic to be kept separate from the storage-layer code in pkg types.
type NomsGeometry types.Geometry

var _ sql.GeometryValue = NomsGeometry{}

// NomsGeometryFromGeometryValue converts a sql.GeometryValue to a NomsGeometry value.
func NomsGeometryFromGeometryValue(ctx context.Context, vrw types.ValueReadWriter, val sql.GeometryValue) (NomsGeometry, error) {
	if noms, ok := val.(NomsGeometry); ok {
		return noms, nil
	}

	sqlObj, err := val.Unmarshall(sql.NewContext(ctx))
	if err != nil {
		return NomsGeometry{}, err
	}

	v, err := marshalGeometry(ctx, vrw, sqlObj.Val)
	if err != nil {
		return NomsGeometry{}, err
	}

	obj, err := types.NewGeometryObj(vrw.Format(), vrw, v)
	if err != nil {
		return NomsGeometry{}, err
	}

	return NomsGeometry(obj), nil
}

// marshalGeometry converts primitive interfaces to something from types package
func marshalGeometry(ctx context.Context, vrw types.ValueReadWriter, val interface{}) (types.Value, error) {
	if val == nil {
		return types.NullValue, nil
	}

	// TODO: how is this supposed to work? Everything under geometry is multiple values?
	switch val := val.(type) {
	case bool: // TODO: this is impossible?
		return types.Bool(val), nil
	case sql.PointType:
		return nil, nil
	default:
		return nil, ErrUnexpectedGeometryTypeIn
	}
}

// Unmarshall implements the sql.Geometry interface.
func (v NomsGeometry) Unmarshall(ctx *sql.Context) (doc sql.GeometryObject, err error) {
	nomsVal, err := types.Geometry(v).Inner()
	if err != nil {
		return sql.GeometryObject{}, err
	}

	val, err := unmarshalGeometry(nomsVal)
	if err != nil {
		return sql.GeometryObject{}, err
	}

	return sql.GeometryObject{Val: val}, nil
}

func unmarshalGeometry(val types.Value) (interface{}, error) {
	//switch val := val.(type) {
	switch val.(type) {
	case types.Null:
		return nil, nil
	default:
		return nil, ErrUnexpectedGeometryTypeIn
	}
}

// Compare implements the sql.GeometryValue interface.
func (v NomsGeometry) Compare(ctx *sql.Context, other sql.GeometryValue) (cmp int, err error) {
	noms, ok := other.(NomsGeometry)
	if !ok {
		doc, err := v.Unmarshall(ctx)
		if err != nil {
			return 0, err
		}
		return doc.Compare(ctx, other)
	}

	return types.Geometry(v).Compare(types.Geometry(noms))
}

// ToString implements the sql.GeometryValue interface.
func (v NomsGeometry) ToString(ctx *sql.Context) (string, error) {
	jd, err := types.Geometry(v).Inner()
	if err != nil {
		return "", err
	}

	sb := &strings.Builder{}
	if err = marshalToString(ctx, sb, jd); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func marshalToString(ctx context.Context, sb *strings.Builder, val types.Value) (err error) {
	//switch val := val.(type) {
	switch val.(type) {
	case types.Null:
		sb.WriteString(GeometryNull)
	default:
		err = ErrUnexpectedGeometryTypeOut
	}
	return
}
*/