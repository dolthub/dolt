package datas

import (
	"sort"

	"github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/gen/fb/serial"
)

type rmentry struct {
	name string
	addr hash.Hash
}

type refmap struct {
	entries []rmentry
}

func (rm refmap) lookup(key string) hash.Hash {
	for _, e := range rm.entries {
		if e.name == key {
			return e.addr
		}
	}
	return hash.Hash{}
}

func (rm *refmap) set(key string, addr hash.Hash) {
	for i := range rm.entries {
		if rm.entries[i].name == key {
			rm.entries[i].addr = addr
			return
		}
	}
	rm.entries = append(rm.entries, rmentry{ key, addr })
}

func (rm *refmap) delete(key string) {
	entries := make([]rmentry, len(rm.entries) - 1)
	j := 0
	for i := range rm.entries {
		if rm.entries[i].name != key {
			entries[j] = rm.entries[i]
			j++
		}
	}
	rm.entries = entries
}

func (rm refmap) flatbuffer() []byte {
	sort.Slice(rm.entries, func(i, j int) bool {
		return rm.entries[i].name < rm.entries[j].name
	})
	builder := flatbuffers.NewBuilder(1024)
	nameoffs := make([]flatbuffers.UOffsetT, len(rm.entries))
	for i := range rm.entries {
		nameoffs[i] = builder.CreateString(rm.entries[i].name)
	}
	serial.RefMapStartNamesVector(builder, len(nameoffs))
	for i := len(nameoffs) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(nameoffs[i])
	}
	namesoff := builder.EndVector(len(nameoffs))

	hashsz := 20
	hashessz := len(rm.entries) * hashsz
	builder.Prep(flatbuffers.SizeUOffsetT, hashessz)
	stop := int(builder.Head())
	start := stop - hashessz
	for _, e := range rm.entries {
		copy(builder.Bytes[start:stop], e.addr[:])
		start += hashsz
	}
	start = stop - hashessz
	refarrayoff := builder.CreateByteVector(builder.Bytes[start:stop])

	serial.RefMapStart(builder)
	serial.RefMapAddNames(builder, namesoff)
	serial.RefMapAddRefArray(builder, refarrayoff)
	serial.RefMapAddTreeCount(builder, uint64(len(rm.entries)))
	serial.RefMapAddTreeLevel(builder, 0)
	builder.Finish(serial.RefMapEnd(builder))
	return builder.FinishedBytes()
}

func parse_refmap(bs []byte) refmap {
	rm := serial.GetRootAsRefMap(bs, 0)
	if rm.TreeLevel() != 0 {
		panic("unsupported multi-level refmap")
	}
	if uint64(rm.NamesLength()) != rm.TreeCount() {
		panic("inconsistent refmap at level 0 where names length != tree count")
	}
	hashsz := 20
	if rm.RefArrayLength() != rm.NamesLength()*hashsz {
		panic("inconsistent refmap at level 0 ref array length length != hashsz * names length")
	}
	entries := make([]rmentry, rm.NamesLength())
	refs := rm.RefArrayBytes()
	for i := 0; i < rm.NamesLength(); i++ {
		entries[i].name = string(rm.Names(i))
		off := i*hashsz
		copy(entries[i].addr[:], refs[off:off+hashsz])
	}
	return refmap{entries}
}
