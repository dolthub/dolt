// Copyright 2019 Liquidata, Inc.
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

import (
	"context"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

// Bool is a Noms Value wrapper around the primitive bool type.
type Bool bool

// Value interface
func (b Bool) Value(ctx context.Context) Value {
	return b
}

func (b Bool) Equals(other Value) bool {
	return b == other
}

func (b Bool) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if b2, ok := other.(Bool); ok {
		return !bool(b) && bool(b2)
	}
	return true
}

func (b Bool) Hash(nbf *NomsBinFormat) hash.Hash {
	return getHash(b, nbf)
}

func (b Bool) WalkValues(ctx context.Context, cb ValueCallback) {
}

func (b Bool) WalkRefs(nbf *NomsBinFormat, cb RefCallback) {
}

func (b Bool) typeOf() *Type {
	return BoolType
}

func (b Bool) Kind() NomsKind {
	return BoolKind
}

func (b Bool) valueReadWriter() ValueReadWriter {
	return nil
}

func (b Bool) writeTo(w nomsWriter, nbf *NomsBinFormat) {
	BoolKind.writeTo(w, nbf)
	w.writeBool(bool(b))
}

func (b Bool) valueBytes(nbf *NomsBinFormat) []byte {
	if bool(b) {
		return []byte{byte(BoolKind), 1}
	}
	return []byte{byte(BoolKind), 0}
}
