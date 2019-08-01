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

package types

import (
	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func mustRef(ref Ref, err error) Ref {
	d.PanicIfError(err)
	return ref
}

func mustValue(val Value, err error) Value {
	d.PanicIfError(err)
	return val
}

func mustBlob(blob Blob, err error) Blob {
	d.PanicIfError(err)
	return blob
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

func mustList(l List, err error) List {
	d.PanicIfError(err)
	return l
}

func mustMap(m Map, err error) Map {
	d.PanicIfError(err)
	return m
}

func mustSet(s Set, err error) Set {
	d.PanicIfError(err)
	return s
}

func mustHash(h hash.Hash, err error) hash.Hash {
	d.PanicIfError(err)
	return h
}

func mustSeq(seq sequence, err error) sequence {
	d.PanicIfError(err)
	return seq
}

func mustMetaTuple(mt metaTuple, err error) metaTuple {
	d.PanicIfError(err)
	return mt
}

func mustOrdKey(ordKey orderedKey, err error) orderedKey {
	d.PanicIfError(err)
	return ordKey
}

func mustOrdSeq(ordSeq orderedSequence, err error) orderedSequence {
	d.PanicIfError(err)
	return ordSeq
}

func mustMIter(itr MapIterator, err error) MapIterator {
	d.PanicIfError(err)
	return itr
}

func mustSIter(itr SetIterator, err error) SetIterator {
	d.PanicIfError(err)
	return itr
}