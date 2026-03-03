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

package types

func MakePrimitiveType(k NomsKind) (*Type, error) {
	if typ, ok := PrimitiveTypeMap[k]; ok {
		return typ, nil
	}
	return nil, ErrUnknownType
}

func makePrimitiveType(k NomsKind) *Type {
	return newType(PrimitiveDesc(k))
}

// PrimitiveTypeMap auto populates with Value types that return true from isPrimitive().
// Only include a type here manually if it has no associated Value type.
var PrimitiveTypeMap = map[NomsKind]*Type{
	ValueKind: makePrimitiveType(ValueKind),
}

func makeCompoundType(kind NomsKind, elemTypes ...*Type) (*Type, error) {
	for _, el := range elemTypes {
		if el.Kind() == UnknownKind {
			return nil, ErrUnknownType
		}
	}

	return newType(CompoundDesc{elemTypes, kind}), nil
}

func makeUnionType(elemTypes ...*Type) (*Type, error) {
	if len(elemTypes) == 1 {
		return elemTypes[0], nil
	}
	return makeCompoundType(UnionKind, elemTypes...)
}

func makeStructTypeQuickly(name string, fields structTypeFields) (*Type, error) {
	for _, fld := range fields {
		if fld.Type.Kind() == UnknownKind {
			return nil, ErrUnknownType
		}
	}

	return newType(StructDesc{name, fields}), nil
}

// StructField describes a field in a struct type.
type StructField struct {
	Type     *Type
	Name     string
	Optional bool
}

type structTypeFields []StructField

func (s structTypeFields) Len() int           { return len(s) }
func (s structTypeFields) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s structTypeFields) Less(i, j int) bool { return s[i].Name < s[j].Name }
