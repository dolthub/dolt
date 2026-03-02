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

package types

import (
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func mustRef(ref Ref, err error) Ref {
	d.PanicIfError(err)
	return ref
}

func mustValue(val Value, err error) Value {
	d.PanicIfError(err)
	return val
}

func mustType(t *Type, err error) *Type {
	d.PanicIfError(err)
	return t
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustStruct(st Struct, err error) Struct {
	d.PanicIfError(err)
	return st
}

func mustHash(h hash.Hash, err error) hash.Hash {
	d.PanicIfError(err)
	return h
}
