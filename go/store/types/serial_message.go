// Copyright 2022 Dolthub, Inc.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

type SerialMessage []byte

func (sm SerialMessage) Kind() NomsKind {
	return SerialMessageKind
}

func (sm SerialMessage) Value(ctx context.Context) (Value, error) {
	return sm, nil
}

func (sm SerialMessage) isPrimitive() bool {
	return true
}

func (sm SerialMessage) Equals(other Value) bool {
	if other.Kind() != SerialMessageKind {
		return false
	}
	return bytes.Equal(sm, other.(SerialMessage))
}

func (sm SerialMessage) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(sm, nbf)
}

func (sm SerialMessage) HumanReadableString() string {
	if serial.GetFileID([]byte(sm)) == serial.StoreRootFileID {
		msg := serial.GetRootAsStoreRoot([]byte(sm), 0)
		ret := &strings.Builder{}
		refs := msg.Refs(nil)
		fmt.Fprintf(ret, "{\n")
		hashes := refs.RefArrayBytes()
		for i := 0; i < refs.NamesLength(); i++ {
			name := refs.Names(i)
			addr := hash.New(hashes[:20])
			fmt.Fprintf(ret, "  %s: %s\n", name, addr.String())
		}
		fmt.Fprintf(ret, "}")
		return ret.String()
	}
	return "SerialMessage"
}

func (sm SerialMessage) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(SerialMessage); ok {
		return bytes.Compare(sm, v2) == -1, nil
	}
	return sm.Kind() < other.Kind(), nil
}

// Refs in SerialMessage do not have height. This should be taller than
// any true Ref height we expect to see in a RootValue.
const SerialMessageRefHeight = 1024

func (sm SerialMessage) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	switch serial.GetFileID([]byte(sm)) {
	case serial.StoreRootFileID:
		msg := serial.GetRootAsStoreRoot([]byte(sm), 0)
		rm := msg.Refs(nil)
		refs := rm.RefArrayBytes()
		for i := 0; i < rm.NamesLength(); i++ {
			off := i * 20
			addr := hash.New(refs[off : off+20])
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
	case serial.TagFileID:
		msg := serial.GetRootAsTag([]byte(sm), 0)
		addr := hash.New(msg.CommitAddrBytes())
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		return cb(r)
	case serial.WorkingSetFileID:
		msg := serial.GetRootAsWorkingSet([]byte(sm), 0)
		addr := hash.New(msg.WorkingRootAddrBytes())
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		if err = cb(r); err != nil {
			return err
		}
		if msg.StagedRootAddrLength() != 0 {
			addr = hash.New(msg.StagedRootAddrBytes())
			r, err = constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
		mergeState := msg.MergeState(nil)
		if mergeState != nil {
			addr = hash.New(mergeState.PreWorkingRootAddrBytes())
			r, err = constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}

			addr = hash.New(mergeState.FromCommitAddrBytes())
			r, err = constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
	case serial.RootValueFileID:
		msg := serial.GetRootAsRootValue([]byte(sm), 0)
		rm := msg.Tables(nil)
		refs := rm.RefArrayBytes()
		for i := 0; i < rm.NamesLength(); i++ {
			off := i * 20
			addr := hash.New(refs[off : off+20])
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
		addr := hash.New(msg.ForeignKeyAddrBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
		addr = hash.New(msg.SuperSchemasAddrBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
	case serial.TableFileID:
		msg := serial.GetRootAsTable([]byte(sm), 0)
		addr := hash.New(msg.SchemaBytes())
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		err = cb(r)
		if err != nil {
			return err
		}

		addr = hash.New(msg.ViolationsBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		confs := msg.Conflicts(nil)
		addr = hash.New(confs.DataBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		addr = hash.New(confs.OurSchemaBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		addr = hash.New(confs.TheirSchemaBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		addr = hash.New(confs.AncestorSchemaBytes())
		if !addr.IsEmpty() {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		rm := msg.SecondaryIndexes(nil)
		refs := rm.RefArrayBytes()
		for i := 0; i < rm.NamesLength(); i++ {
			off := i * 20
			addr := hash.New(refs[off : off+20])
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}

		mapbytes := msg.PrimaryIndexBytes()

		if nbf == Format_DOLT_DEV {
			dec := newValueDecoder(mapbytes, nil)
			v, err := dec.readValue(nbf)
			if err != nil {
				return err
			}
			return v.walkRefs(nbf, cb)
		} else {
			return TupleRowStorage(mapbytes).walkRefs(nbf, cb)
		}
	case serial.CommitFileID:
		parents, err := SerialCommitParentAddrs(nbf, sm)
		if err != nil {
			return err
		}
		for _, addr := range parents {
			r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
			if err != nil {
				return err
			}
			if err = cb(r); err != nil {
				return err
			}
		}
		msg := serial.GetRootAsCommit([]byte(sm), 0)
		addr := hash.New(msg.RootBytes())
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		if err = cb(r); err != nil {
			return err
		}
		// TODO: cb for parent closure.
	default:
		return fmt.Errorf("unsupported SerialMessage message with FileID: %s", serial.GetFileID([]byte(sm)))
	}
	return nil
}

func SerialCommitParentAddrs(nbf *NomsBinFormat, sm SerialMessage) ([]hash.Hash, error) {
	msg := serial.GetRootAsCommit([]byte(sm), 0)
	addrs := msg.ParentAddrsBytes()
	n := len(addrs) / 20
	ret := make([]hash.Hash, n)
	for i := 0; i < n; i++ {
		addr := hash.New(addrs[:20])
		addrs = addrs[20:]
		ret[i] = addr
	}
	return ret, nil
}

func (sm SerialMessage) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	bytes := b.ReadInlineBlob()
	return SerialMessage(bytes), nil
}

func (sm SerialMessage) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	size := uint32(b.readUint16())
	b.skipBytes(size)
}

func (sm SerialMessage) typeOf() (*Type, error) {
	return PrimitiveTypeMap[SerialMessageKind], nil
}

func (sm SerialMessage) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	byteLen := len(sm)
	if byteLen > math.MaxUint16 {
		return fmt.Errorf("SerialMessage has length %v when max is %v", byteLen, math.MaxUint16)
	}

	err := SerialMessageKind.writeTo(w, nbf)
	if err != nil {
		return err
	}
	w.writeUint16(uint16(byteLen))
	w.writeRaw(sm)
	return nil
}

func (sm SerialMessage) valueReadWriter() ValueReadWriter {
	return nil
}
