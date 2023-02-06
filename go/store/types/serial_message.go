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
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
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
	return hash.Of(sm), nil
}

func (sm SerialMessage) HumanReadableString() string {
	id := serial.GetFileID(sm)
	switch id {
	// NOTE: splunk uses a separate path for some printing
	case serial.StoreRootFileID:
		msg := serial.GetRootAsStoreRoot([]byte(sm), serial.MessagePrefixSz)
		ret := &strings.Builder{}
		mapbytes := msg.AddressMapBytes()
		fmt.Fprintf(ret, "StoreRoot{%s}", SerialMessage(mapbytes).HumanReadableString())
		return ret.String()
	case serial.TagFileID:
		return "Tag"
	case serial.WorkingSetFileID:
		msg := serial.GetRootAsWorkingSet(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		fmt.Fprintf(ret, "{\n")
		fmt.Fprintf(ret, "\tName: %s\n", msg.Name())
		fmt.Fprintf(ret, "\tDesc: %s\n", msg.Desc())
		fmt.Fprintf(ret, "\tEmail: %s\n", msg.Email())
		fmt.Fprintf(ret, "\tTime: %s\n", time.UnixMilli((int64)(msg.TimestampMillis())).String())
		fmt.Fprintf(ret, "\tWorkingRootAddr: #%s\n", hash.New(msg.WorkingRootAddrBytes()).String())
		fmt.Fprintf(ret, "\tStagedRootAddr: #%s\n", hash.New(msg.StagedRootAddrBytes()).String())
		fmt.Fprintf(ret, "}")
		return ret.String()
	case serial.CommitFileID:
		msg := serial.GetRootAsCommit(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		fmt.Fprintf(ret, "{\n")
		fmt.Fprintf(ret, "\tName: %s\n", msg.Name())
		fmt.Fprintf(ret, "\tDesc: %s\n", msg.Description())
		fmt.Fprintf(ret, "\tEmail: %s\n", msg.Email())
		fmt.Fprintf(ret, "\tTime: %s\n", time.UnixMilli((int64)(msg.TimestampMillis())).String())
		fmt.Fprintf(ret, "\tHeight: %d\n", msg.Height())

		fmt.Fprintf(ret, "\tRootValue: {\n")
		hashes := msg.RootBytes()
		for i := 0; i < len(hashes)/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*20 : (i+1)*20])
			fmt.Fprintf(ret, "\t\t#%s\n", addr.String())
		}
		fmt.Fprintf(ret, "\t}\n")

		fmt.Fprintf(ret, "\tParents: {\n")
		hashes = msg.ParentAddrsBytes()
		for i := 0; i < msg.ParentAddrsLength()/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*20 : (i+1)*20])
			fmt.Fprintf(ret, "\t\t#%s\n", addr.String())
		}
		fmt.Fprintf(ret, "\t}\n")

		fmt.Fprintf(ret, "\tParentClosure: {\n")
		hashes = msg.ParentClosureBytes()
		for i := 0; i < msg.ParentClosureLength()/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*20 : (i+1)*20])
			fmt.Fprintf(ret, "\t\t#%s\n", addr.String())
		}
		fmt.Fprintf(ret, "\t}\n")

		fmt.Fprintf(ret, "}")
		return ret.String()
	case serial.RootValueFileID:
		msg := serial.GetRootAsRootValue(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		fmt.Fprintf(ret, "{\n")
		fmt.Fprintf(ret, "\tFeatureVersion: %d\n", msg.FeatureVersion())
		fmt.Fprintf(ret, "\tForeignKeys: #%s\n", hash.New(msg.ForeignKeyAddrBytes()).String())
		fmt.Fprintf(ret, "\tTables: {\n\t%s", SerialMessage(msg.TablesBytes()).HumanReadableString())
		fmt.Fprintf(ret, "\t}\n")
		fmt.Fprintf(ret, "}")
		return ret.String()
	case serial.TableFileID:
		msg := serial.GetRootAsTable(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}

		fmt.Fprintf(ret, "asdasdf {\n")
		fmt.Fprintf(ret, "\tSchema: #%s\n", hash.New(msg.SchemaBytes()).String())
		fmt.Fprintf(ret, "\tViolations: #%s\n", hash.New(msg.ViolationsBytes()).String())
		// TODO: merge conflicts, not stable yet

		fmt.Fprintf(ret, "\tAutoinc: %d\n", msg.AutoIncrementValue())

		// TODO: can't use tree package to print here, creates a cycle
		fmt.Fprintf(ret, "\tPrimary index: prolly tree\n")

		fmt.Fprintf(ret, "\tSecondary indexes: {\n\t%s\n", SerialMessage(msg.SecondaryIndexesBytes()).HumanReadableString())
		fmt.Fprintf(ret, "\t}\n")
		fmt.Fprintf(ret, "}")
		return ret.String()
	case serial.AddressMapFileID:
		keys, values, _, cnt, err := message.UnpackFields(serial.Message(sm))
		if err != nil {
			return fmt.Sprintf("error in HumanReadString(): %s", err)
		}
		var b strings.Builder
		b.Write([]byte("AddressMap{\n"))
		for i := uint16(0); i < cnt; i++ {
			name := keys.GetItem(int(i), serial.Message(sm))
			addr := values.GetItem(int(i), serial.Message(sm))
			b.Write([]byte("\t"))
			b.Write(name)
			b.Write([]byte(": #"))
			b.Write([]byte(hash.New(addr).String()))
			b.Write([]byte("\n"))
		}
		b.Write([]byte("}"))
		return b.String()
	default:
		return fmt.Sprintf("SerialMessage (HumanReadableString not implemented), [%v]: %s", id, strings.ToUpper(hex.EncodeToString(sm)))
	}
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
	return sm.WalkAddrs(nbf, func(addr hash.Hash) error {
		r, err := constructRef(nbf, addr, PrimitiveTypeMap[ValueKind], SerialMessageRefHeight)
		if err != nil {
			return err
		}
		return cb(r)
	})
}
func (sm SerialMessage) WalkAddrs(nbf *NomsBinFormat, cb func(addr hash.Hash) error) error {
	switch serial.GetFileID(sm) {
	case serial.StoreRootFileID:
		var msg serial.StoreRoot
		err := serial.InitStoreRootRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		if msg.AddressMapLength() > 0 {
			mapbytes := msg.AddressMapBytes()
			return SerialMessage(mapbytes).WalkAddrs(nbf, cb)
		}
	case serial.TagFileID:
		var msg serial.Tag
		err := serial.InitTagRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		return cb(hash.New(msg.CommitAddrBytes()))
	case serial.WorkingSetFileID:
		var msg serial.WorkingSet
		err := serial.InitWorkingSetRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		if err = cb(hash.New(msg.WorkingRootAddrBytes())); err != nil {
			return err
		}
		if msg.StagedRootAddrLength() != 0 {
			if err = cb(hash.New(msg.StagedRootAddrBytes())); err != nil {
				return err
			}
		}
		mergeState := msg.MergeState(nil)
		if mergeState != nil {
			if err = cb(hash.New(mergeState.PreWorkingRootAddrBytes())); err != nil {
				return err
			}
			if err = cb(hash.New(mergeState.FromCommitAddrBytes())); err != nil {
				return err
			}
		}
	case serial.RootValueFileID:
		var msg serial.RootValue
		err := serial.InitRootValueRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		err = SerialMessage(msg.TablesBytes()).WalkAddrs(nbf, cb)
		if err != nil {
			return err
		}
		addr := hash.New(msg.ForeignKeyAddrBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}
	case serial.TableFileID:
		var msg serial.Table
		err := serial.InitTableRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		err = cb(hash.New(msg.SchemaBytes()))
		if err != nil {
			return err
		}

		confs := msg.Conflicts(nil)
		addr := hash.New(confs.DataBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		addr = hash.New(confs.OurSchemaBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		addr = hash.New(confs.TheirSchemaBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		addr = hash.New(confs.AncestorSchemaBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		addr = hash.New(msg.ViolationsBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		addr = hash.New(msg.ArtifactsBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}

		err = SerialMessage(msg.SecondaryIndexesBytes()).WalkAddrs(nbf, cb)
		if err != nil {
			return err
		}

		mapbytes := msg.PrimaryIndexBytes()

		if nbf == Format_DOLT_DEV {
			dec := newValueDecoder(mapbytes, nil)
			v, err := dec.readValue(nbf)
			if err != nil {
				return err
			}
			return v.walkRefs(nbf, func(ref Ref) error {
				return cb(ref.TargetHash())
			})
		} else {
			return SerialMessage(mapbytes).WalkAddrs(nbf, cb)
		}
	case serial.CommitFileID:
		parents, err := SerialCommitParentAddrs(nbf, sm)
		if err != nil {
			return err
		}
		for _, addr := range parents {
			if err = cb(addr); err != nil {
				return err
			}
		}
		var msg serial.Commit
		err = serial.InitCommitRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		addr := hash.New(msg.RootBytes())
		if err = cb(addr); err != nil {
			return err
		}

		addr = hash.New(msg.ParentClosureBytes())
		if !addr.IsEmpty() {
			if err = cb(addr); err != nil {
				return err
			}
		}
	case serial.TableSchemaFileID, serial.ForeignKeyCollectionFileID:
		// no further references from these file types
		return nil
	case serial.ProllyTreeNodeFileID, serial.AddressMapFileID, serial.MergeArtifactsFileID, serial.BlobFileID, serial.CommitClosureFileID:
		return message.WalkAddresses(context.TODO(), serial.Message(sm), func(ctx context.Context, addr hash.Hash) error {
			return cb(addr)
		})
	default:
		return fmt.Errorf("unsupported SerialMessage message with FileID: %s", serial.GetFileID(sm))
	}
	return nil
}

func SerialCommitParentAddrs(nbf *NomsBinFormat, sm SerialMessage) ([]hash.Hash, error) {
	var msg serial.Commit
	err := serial.InitCommitRoot(&msg, []byte(sm), serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}
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
	bytes := b.readSerialMessage()
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
	w.writeRaw(sm)
	return nil
}

func (sm SerialMessage) valueReadWriter() ValueReadWriter {
	return nil
}
