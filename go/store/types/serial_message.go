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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/store/val"

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
	return sm.HumanReadableStringAtIndentationLevel(0)
}

func printWithIndendationLevel(level int, builder *strings.Builder, format string, a ...any) {
	fmt.Fprintf(builder, strings.Repeat("\t", level))
	fmt.Fprintf(builder, format, a...)
}

func (sm SerialMessage) HumanReadableStringAtIndentationLevel(level int) string {
	id := serial.GetFileID(sm)
	switch id {
	// NOTE: splunk uses a separate path for some printing
	// NOTE: We ignore the errors from field number checks here...
	case serial.StoreRootFileID:
		msg, _ := serial.TryGetRootAsStoreRoot([]byte(sm), serial.MessagePrefixSz)
		ret := &strings.Builder{}
		mapbytes := msg.AddressMapBytes()
		printWithIndendationLevel(level, ret, "StoreRoot{%s}",
			SerialMessage(mapbytes).HumanReadableStringAtIndentationLevel(level+1))
		return ret.String()
	case serial.StashListFileID:
		msg, _ := serial.TryGetRootAsStashList([]byte(sm), serial.MessagePrefixSz)
		ret := &strings.Builder{}
		mapbytes := msg.AddressMapBytes()
		printWithIndendationLevel(level, ret, "StashList{%s}",
			SerialMessage(mapbytes).HumanReadableStringAtIndentationLevel(level+1))
		return ret.String()
	case serial.StashFileID:
		msg, _ := serial.TryGetRootAsStash(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tBranchName: %s\n", msg.BranchName())
		printWithIndendationLevel(level, ret, "\tDesc: %s\n", msg.Desc())
		printWithIndendationLevel(level, ret, "\tStashRootAddr: #%s\n", hash.New(msg.StashRootAddrBytes()).String())
		printWithIndendationLevel(level, ret, "\tHeadCommitAddr: #%s\n", hash.New(msg.HeadCommitAddrBytes()).String())
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.StatisticFileID:
		msg, _ := serial.TryGetRootAsStatistic(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tStatsRoot: #%s\n", hash.New(msg.RootBytes()).String())
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.TagFileID:
		msg, _ := serial.TryGetRootAsTag(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tName: %s\n", msg.Name())
		printWithIndendationLevel(level, ret, "\tDesc: %s\n", msg.Desc())
		printWithIndendationLevel(level, ret, "\tEmail: %s\n", msg.Email())
		printWithIndendationLevel(level, ret, "\tUserTimestamp: %d\n", msg.UserTimestampMillis())
		printWithIndendationLevel(level, ret, "\tCommitAddress: #%s\n", hash.New(msg.CommitAddrBytes()).String())
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.WorkingSetFileID:
		msg, _ := serial.TryGetRootAsWorkingSet(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tName: %s\n", msg.Name())
		printWithIndendationLevel(level, ret, "\tDesc: %s\n", msg.Desc())
		printWithIndendationLevel(level, ret, "\tEmail: %s\n", msg.Email())
		printWithIndendationLevel(level, ret, "\tTime: %s\n", time.UnixMilli((int64)(msg.TimestampMillis())).String())
		printWithIndendationLevel(level, ret, "\tWorkingRootAddr: #%s\n", hash.New(msg.WorkingRootAddrBytes()).String())
		printWithIndendationLevel(level, ret, "\tStagedRootAddr: #%s\n", hash.New(msg.StagedRootAddrBytes()).String())
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.CommitFileID:
		msg, _ := serial.TryGetRootAsCommit(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tName: %s\n", msg.Name())
		printWithIndendationLevel(level, ret, "\tDesc: %s\n", msg.Description())
		printWithIndendationLevel(level, ret, "\tEmail: %s\n", msg.Email())
		printWithIndendationLevel(level, ret, "\tTimestamp: %s\n", time.UnixMilli((int64)(msg.TimestampMillis())).String())
		printWithIndendationLevel(level, ret, "\tUserTimestamp: %s\n", time.UnixMilli(msg.UserTimestampMillis()).String())
		printWithIndendationLevel(level, ret, "\tHeight: %d\n", msg.Height())

		printWithIndendationLevel(level, ret, "\tRootValue: {\n")
		hashes := msg.RootBytes()
		for i := 0; i < len(hashes)/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*hash.ByteLen : (i+1)*hash.ByteLen])
			printWithIndendationLevel(level, ret, "\t\t#%s\n", addr.String())
		}
		printWithIndendationLevel(level, ret, "\t}\n")

		printWithIndendationLevel(level, ret, "\tParents: {\n")
		hashes = msg.ParentAddrsBytes()
		for i := 0; i < msg.ParentAddrsLength()/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*hash.ByteLen : (i+1)*hash.ByteLen])
			printWithIndendationLevel(level, ret, "\t\t#%s\n", addr.String())
		}
		printWithIndendationLevel(level, ret, "\t}\n")

		printWithIndendationLevel(level, ret, "\tParentClosure: {\n")
		hashes = msg.ParentClosureBytes()
		for i := 0; i < msg.ParentClosureLength()/hash.ByteLen; i++ {
			addr := hash.New(hashes[i*hash.ByteLen : (i+1)*hash.ByteLen])
			printWithIndendationLevel(level, ret, "\t\t#%s\n", addr.String())
		}
		printWithIndendationLevel(level, ret, "\t}\n")

		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.RootValueFileID:
		msg, _ := serial.TryGetRootAsRootValue(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tFeatureVersion: %d\n", msg.FeatureVersion())
		printWithIndendationLevel(level, ret, "\tForeignKeys: #%s\n", hash.New(msg.ForeignKeyAddrBytes()).String())
		printWithIndendationLevel(level, ret, "\tTables: %s\n",
			SerialMessage(msg.TablesBytes()).HumanReadableStringAtIndentationLevel(level+1))
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.DoltgresRootValueFileID:
		return DoltgresRootValueHumanReadableStringAtIndentationLevel(sm, level)
	case serial.TableFileID:
		msg, _ := serial.TryGetRootAsTable(sm, serial.MessagePrefixSz)
		ret := &strings.Builder{}

		printWithIndendationLevel(level, ret, "{\n")
		printWithIndendationLevel(level, ret, "\tSchema: #%s\n", hash.New(msg.SchemaBytes()).String())
		printWithIndendationLevel(level, ret, "\tViolations: #%s\n", hash.New(msg.ViolationsBytes()).String())
		// TODO: merge conflicts, not stable yet

		printWithIndendationLevel(level, ret, "\tAutoinc: %d\n", msg.AutoIncrementValue())

		printWithIndendationLevel(level, ret, "\tPrimary index: #%s\n", hash.Of(msg.PrimaryIndexBytes()))
		printWithIndendationLevel(level, ret, "\tSecondary indexes: %s\n",
			SerialMessage(msg.SecondaryIndexesBytes()).HumanReadableStringAtIndentationLevel(level+1))
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.AddressMapFileID:
		keys, values, _, cnt, err := message.UnpackFields(serial.Message(sm))
		if err != nil {
			return fmt.Sprintf("error in HumanReadString(): %s", err)
		}
		var b strings.Builder
		b.Write([]byte("AddressMap {\n"))
		for i := uint16(0); i < cnt; i++ {
			name := keys.GetItem(int(i), serial.Message(sm))
			addr := values.GetItem(int(i), serial.Message(sm))
			b.Write([]byte(strings.Repeat("\t", level+1)))
			b.Write(name)
			b.Write([]byte(": #"))
			b.Write([]byte(hash.New(addr).String()))
			b.Write([]byte("\n"))
		}
		b.Write([]byte(strings.Repeat("\t", level)))
		b.Write([]byte("}"))
		return b.String()
	case serial.CommitClosureFileID:
		msg, _ := serial.TryGetRootAsCommitClosure(sm, serial.MessagePrefixSz)

		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		level += 1

		printWithIndendationLevel(level, ret, "SubTree {\n")
		level += 1
		addresses := msg.AddressArrayBytes()
		for i := 0; i < len(addresses)/hash.ByteLen; i++ {
			addr := hash.New(addresses[i*hash.ByteLen : (i+1)*hash.ByteLen])
			printWithIndendationLevel(level, ret, "#%s\n", addr.String())
		}
		level -= 1
		printWithIndendationLevel(level, ret, "}\n")

		printWithIndendationLevel(level, ret, "Commits {\n")
		level += 1
		if msg.TreeLevel() == 0 {
			// If Level() == 0, we're at the leaf level, so print the key items.
			keybytes := msg.KeyItemsBytes()
			// Magic numbers: 8 bytes (uint64) for height, 20 bytes (hash.ByteLen) for address
			for i := 0; i < len(keybytes); i += 28 {
				height := binary.LittleEndian.Uint64(keybytes[i : i+8])
				addr := hash.New(keybytes[(i + 8) : (i+8)+hash.ByteLen])
				printWithIndendationLevel(level, ret, "#%s (height: %d)\n", addr.String(), height)
			}
		}
		level -= 1
		printWithIndendationLevel(level, ret, "} \n")
		level -= 1
		printWithIndendationLevel(level, ret, "}\n")
		return ret.String()
	case serial.TableSchemaFileID:
		msg, _ := serial.TryGetRootAsTableSchema(sm, serial.MessagePrefixSz)

		// loop over columns
		columns := msg.ColumnsLength()
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		level += 1
		printWithIndendationLevel(level, ret, "Columns: [\n")
		level += 1
		for i := 0; i < columns; i++ {
			printWithIndendationLevel(level, ret, "{\n")
			level += 1
			col := serial.Column{}
			ok, err := msg.TryColumns(&col, i)
			if err != nil {
				return fmt.Sprintf("error in HumanReadString(): %s", err)
			}
			if !ok {
				return fmt.Sprintf("error in HumanReadString(): could not get column %d", i)
			}

			printWithIndendationLevel(level, ret, "Name: %s\n", col.Name())
			printWithIndendationLevel(level, ret, "SQLType: %s\n", col.SqlType())
			printWithIndendationLevel(level, ret, "DefaultValue: %s\n", col.DefaultValue())
			printWithIndendationLevel(level, ret, "Comment: %s\n", col.Comment())
			printWithIndendationLevel(level, ret, "DisplayOrder: %d\n", col.DisplayOrder())
			printWithIndendationLevel(level, ret, "Tag: %d\n", col.Tag())
			printWithIndendationLevel(level, ret, "Encoding: %s\n", col.Encoding())
			printWithIndendationLevel(level, ret, "Primary Key: %t\n", col.PrimaryKey())
			printWithIndendationLevel(level, ret, "Nullable: %t\n", col.Nullable())
			printWithIndendationLevel(level, ret, "Auto Increment: %t\n", col.AutoIncrement())
			printWithIndendationLevel(level, ret, "Hidden: %t\n", col.Hidden())
			printWithIndendationLevel(level, ret, "Generated: %t\n", col.Generated())
			printWithIndendationLevel(level, ret, "Virtual: %t\n", col.Virtual())
			printWithIndendationLevel(level, ret, "OnUpdateValue: %s\n", col.OnUpdateValue())
			level -= 1
			printWithIndendationLevel(level, ret, "}\n")

		}
		level -= 1
		printWithIndendationLevel(level, ret, "]\n")

		level -= 1
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.ProllyTreeNodeFileID:
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		level++

		_ = OutputProllyNodeBytes(ret, serial.Message(sm))

		level -= 1
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	case serial.BlobFileID:
		ret := &strings.Builder{}
		printWithIndendationLevel(level, ret, "{\n")
		level++
		_ = OutputBlobNodeBytes(ret, level, serial.Message(sm))
		level -= 1
		printWithIndendationLevel(level, ret, "}")
		return ret.String()
	default:

		return fmt.Sprintf("SerialMessage (HumanReadableString not implemented), [%v]: %s", id, strings.ToUpper(hex.EncodeToString(sm)))
	}
}

func OutputBlobNodeBytes(w *strings.Builder, indentationLevel int, msg serial.Message) error {
	keys, values, treeLevel, count, err := message.UnpackFields(msg)
	if err != nil {
		return err
	}
	isLeaf := treeLevel == 0

	if isLeaf {
		printWithIndendationLevel(indentationLevel, w, "Blob - ")
		w.Write(values.GetItem(0, msg))
		w.WriteString("\n")
		return nil
	}

	for i := 0; i < int(count); i++ {
		k := keys.GetItem(i, msg)
		kt := val.Tuple(k)

		w.Write([]byte("\n    { key: "))
		for j := 0; j < kt.Count(); j++ {
			if j > 0 {
				w.Write([]byte(", "))
			}

			w.Write([]byte(hex.EncodeToString(kt.GetField(j))))
		}

		ref := hash.New(values.GetItem(i, msg))

		w.Write([]byte(" ref: #"))
		w.Write([]byte(ref.String()))
		w.Write([]byte(" }"))
	}

	w.Write([]byte("\n"))
	return nil
}

func OutputProllyNodeBytes(w io.Writer, msg serial.Message) error {
	keys, values, treeLevel, count, err := message.UnpackFields(msg)
	if err != nil {
		return err
	}
	isLeaf := treeLevel == 0

	node, err := serial.TryGetRootAsProllyTreeNode(msg, serial.MessagePrefixSz)
	if err != nil {
		return err
	}

	addresses := make([][]byte, node.ValueAddressOffsetsLength())
	for i := 0; i < node.ValueAddressOffsetsLength(); i++ {
		offset := node.ValueAddressOffsets(i)
		addresses[i] = node.ValueItemsBytes()[offset : offset+20]
	}

	for i := 0; i < int(count); i++ {
		k := keys.GetItem(i, msg)
		kt := val.Tuple(k)

		w.Write([]byte("\n    { key: "))
		for j := 0; j < kt.Count(); j++ {
			if j > 0 {
				w.Write([]byte(", "))
			}

			w.Write([]byte(hex.EncodeToString(kt.GetField(j))))
		}

		if isLeaf {
			v := values.GetItem(i, msg)
			vt := val.Tuple(v)

			w.Write([]byte(" value: "))
			for j := 0; j < vt.Count(); j++ {
				if j > 0 {
					w.Write([]byte(", "))
				}
				field := vt.GetField(j)
				if len(field) == hash.ByteLen {
					// This value may be an address. Check to see if it's in `addresses`
					isAddress := slices.ContainsFunc(addresses, func(address []byte) bool {
						return slices.Equal(address, field)
					})
					if isAddress {
						ref := hash.New(field)

						w.Write([]byte(" #"))
						w.Write([]byte(ref.String()))
						continue
					}
				}
				w.Write([]byte(hex.EncodeToString(field)))
			}

			w.Write([]byte(" }"))
		} else {
			ref := hash.New(values.GetItem(i, msg))

			w.Write([]byte(" ref: #"))
			w.Write([]byte(ref.String()))
			w.Write([]byte(" }"))
		}
	}

	w.Write([]byte("\n"))
	return nil
}

func (sm SerialMessage) Less(ctx context.Context, nbf *NomsBinFormat, other LesserValuable) (bool, error) {
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
	case serial.StashListFileID:
		var msg serial.StashList
		err := serial.InitStashListRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		if msg.AddressMapLength() > 0 {
			mapbytes := msg.AddressMapBytes()
			return SerialMessage(mapbytes).WalkAddrs(nbf, cb)
		}
	case serial.StatisticFileID:
		var msg serial.Statistic
		err := serial.InitStatisticRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		return cb(hash.New(msg.RootBytes()))
	case serial.StashFileID:
		var msg serial.Stash
		err := serial.InitStashRoot(&msg, []byte(sm), serial.MessagePrefixSz)
		if err != nil {
			return err
		}
		if err = cb(hash.New(msg.StashRootAddrBytes())); err != nil {
			return err
		}
		if err = cb(hash.New(msg.HeadCommitAddrBytes())); err != nil {
			return err
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
		mergeState, err := msg.TryMergeState(nil)
		if err != nil {
			return err
		}
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
	case serial.DoltgresRootValueFileID:
		if !nbf.UsesFlatbuffers() {
			return fmt.Errorf("root values for Doltgres only use flatbuffer serialization")
		}
		return DoltgresRootValueWalkAddrs(sm, cb)
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

		confs, err := msg.TryConflicts(nil)
		if err != nil {
			return err
		}
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
		return SerialMessage(mapbytes).WalkAddrs(nbf, cb)
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
	n := len(addrs) / hash.ByteLen
	ret := make([]hash.Hash, n)
	for i := 0; i < n; i++ {
		addr := hash.New(addrs[:hash.ByteLen])
		addrs = addrs[hash.ByteLen:]
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

// DoltgresRootValueHumanReadableStringAtIndentationLevel returns the human readable string at the given indentation
// level for root values. This is a variable as it's changed in Doltgres.
var DoltgresRootValueHumanReadableStringAtIndentationLevel = func(sm SerialMessage, level int) string {
	return "DOLTGRES ROOT VALUE"
}

// DoltgresRootValueWalkAddrs walks the given message using the given callback. This is a variable as it's changed in
// Doltgres.
var DoltgresRootValueWalkAddrs = func(sm SerialMessage, cb func(addr hash.Hash) error) error {
	return fmt.Errorf("cannot walk a Doltgres root value from within Dolt")
}
