// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type TypeCache struct {
	identTable *identTable
	trieRoots  map[NomsKind]*typeTrie
	nextId     uint32
	mu         *sync.Mutex
}

var staticTypeCache = NewTypeCache()

func makePrimitiveType(k NomsKind) *Type {
	return newType(PrimitiveDesc(k), uint32(k))
}

var BoolType = makePrimitiveType(BoolKind)
var NumberType = makePrimitiveType(NumberKind)
var StringType = makePrimitiveType(StringKind)
var BlobType = makePrimitiveType(BlobKind)
var TypeType = makePrimitiveType(TypeKind)
var ValueType = makePrimitiveType(ValueKind)

func NewTypeCache() *TypeCache {
	return &TypeCache{
		newIdentTable(),
		map[NomsKind]*typeTrie{
			ListKind:   newTypeTrie(),
			SetKind:    newTypeTrie(),
			RefKind:    newTypeTrie(),
			MapKind:    newTypeTrie(),
			StructKind: newTypeTrie(),
			CycleKind:  newTypeTrie(),
			UnionKind:  newTypeTrie(),
		},
		256, // The first 255 type ids are reserved for the 8bit space of NomsKinds.
		&sync.Mutex{},
	}
}

func (tc *TypeCache) Lock() {
	tc.mu.Lock()
}

func (tc *TypeCache) Unlock() {
	tc.mu.Unlock()
}

func (tc *TypeCache) nextTypeId() uint32 {
	next := tc.nextId
	tc.nextId++
	return next
}

func (tc *TypeCache) getCompoundType(kind NomsKind, elemTypes ...*Type) *Type {
	trie := tc.trieRoots[kind]
	for _, t := range elemTypes {
		trie = trie.Traverse(t.id)
	}

	if trie.t == nil {
		trie.t = newType(CompoundDesc{kind, elemTypes}, tc.nextTypeId())
	}

	return trie.t
}

func (tc *TypeCache) makeStructType(name string, fieldNames []string, fieldTypes []*Type) *Type {
	d.PanicIfFalse(len(fieldNames) == len(fieldTypes), "len(fieldNames) != len(fieldTypes)")
	verifyStructName(name)
	verifyFieldNames(fieldNames)

	trie := tc.trieRoots[StructKind].Traverse(tc.identTable.GetId(name))
	for i, fn := range fieldNames {
		ft := fieldTypes[i]
		trie = trie.Traverse(tc.identTable.GetId(fn))
		trie = trie.Traverse(ft.id)
	}

	if trie.t == nil {
		fs := make(fieldSlice, len(fieldNames))
		for i, fn := range fieldNames {
			fs[i] = field{fn, fieldTypes[i]}
			i++
		}

		t := newType(StructDesc{name, fs}, 0)
		if t.HasUnresolvedCycle() {
			t, _ = toUnresolvedType(t, tc, -1, nil)
			resolveStructCycles(t, nil)
			if !t.HasUnresolvedCycle() {
				normalize(t)
			}
		}
		t.id = tc.nextTypeId()
		trie.t = t
	}

	return trie.t
}

func indexOfType(t *Type, tl []*Type) (uint32, bool) {
	for i, tt := range tl {
		if tt == t {
			return uint32(i), true
		}
	}
	return 0, false
}

// Returns a new type where cyclic pointer references are replaced with Cycle<N> types.
func toUnresolvedType(t *Type, tc *TypeCache, level int, parentStructTypes []*Type) (*Type, bool) {
	i, found := indexOfType(t, parentStructTypes)
	if found {
		cycle := CycleDesc(uint32(len(parentStructTypes)) - i - 1)
		return newType(cycle, 0xfa15e), true // This type is just a placeholder. It doesn't need a real id.
	}

	switch desc := t.Desc.(type) {
	case CompoundDesc:
		ts := make(typeSlice, len(desc.ElemTypes))
		didChange := false
		for i, et := range desc.ElemTypes {
			st, changed := toUnresolvedType(et, tc, level, parentStructTypes)
			ts[i] = st
			didChange = didChange || changed
		}

		if !didChange {
			return t, false
		}

		return newType(CompoundDesc{t.Kind(), ts}, tc.nextTypeId()), true
	case StructDesc:
		fs := make(fieldSlice, len(desc.fields))
		didChange := false
		for i, f := range desc.fields {
			fs[i].name = f.name
			st, changed := toUnresolvedType(f.t, tc, level+1, append(parentStructTypes, t))
			fs[i].t = st
			didChange = didChange || changed
		}

		if !didChange {
			return t, false
		}

		return newType(StructDesc{desc.Name, fs}, tc.nextTypeId()), true
	case CycleDesc:
		cycleLevel := int(desc)
		return t, cycleLevel <= level // Only cycles which can be resolved in the current struct.
	}

	return t, false
}

// Drops cycles and replaces them with pointers to parent structs
func resolveStructCycles(t *Type, parentStructTypes []*Type) *Type {
	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for i, et := range desc.ElemTypes {
			desc.ElemTypes[i] = resolveStructCycles(et, parentStructTypes)
		}

	case StructDesc:
		for i, f := range desc.fields {
			desc.fields[i].t = resolveStructCycles(f.t, append(parentStructTypes, t))
		}

	case CycleDesc:
		idx := uint32(desc)
		if idx < uint32(len(parentStructTypes)) {
			return parentStructTypes[uint32(len(parentStructTypes))-1-idx]
		}
	}

	return t
}

// We normalize structs during their construction iff they have no unresolved cycles. Normalizing applies a canonical ordering to the composite types of a union and serializes all types under the struct. To ensure a consistent ordering of the composite types of a union, we generate a unique "order id" or OID for each of those types. The OID is the hash of a unique type encoding that is independent of the extant order of types within any subordinate unions. This encoding for most types is a straightforward serialization of its components; for unions the encoding is a bytewise XOR of the hashes of each of its composite type encodings.
// We require a consistent order of types within a union to ensure that equivalent types have a single persistent encoding and, therefore, a single hash. The method described above fails for "unrolled" cycles whereby two equivalent, but uniquely described structures, would have different OIDs. Consider for example the following two types that, while equivalent, do not yield the same OID:
//	Struct A { a: Cycle<0> }
//	Struct A { a: Struct A { a: Cycle<1> } }
// We explicitly disallow this sort of redundantly expressed type. If a non-Byzantine use of such a construction arises, we can attempt to simplify the expansive type or find another means of comparison.
func normalize(t *Type) {
	walkType(t, nil, func(tt *Type, _ []*Type) {
		generateOID(tt, false)
	})

	walkType(t, nil, func(tt *Type, parentStructTypes []*Type) {
		if tt.Kind() == StructKind {
			for _, ttt := range parentStructTypes {
				if *tt.oid == *ttt.oid {
					panic("unrolled cycle types are not supported; ahl owes you a beer")
				}
			}
		}
	})

	walkType(t, nil, func(tt *Type, _ []*Type) {
		if tt.Kind() == UnionKind {
			sort.Sort(tt.Desc.(CompoundDesc).ElemTypes)
		}
	})

	walkType(t, nil, func(tt *Type, _ []*Type) {
		serializeType(tt)
	})
}

func walkType(t *Type, parentStructTypes []*Type, do func(*Type, []*Type)) {
	if t.Kind() == StructKind {
		if _, found := indexOfType(t, parentStructTypes); found {
			return
		}
	}

	do(t, parentStructTypes)

	switch desc := t.Desc.(type) {
	case CompoundDesc:
		for _, tt := range desc.ElemTypes {
			walkType(tt, parentStructTypes, do)
		}
	case StructDesc:
		for _, f := range desc.fields {
			walkType(f.t, append(parentStructTypes, t), do)
		}
	}
}

func generateOID(t *Type, allowUnresolvedCycles bool) {
	buf := newBinaryNomsWriter()
	encodeForOID(t, buf, allowUnresolvedCycles, t, nil)
	oid := hash.FromData(buf.data())
	t.oid = &oid
}

func encodeForOID(t *Type, buf nomsWriter, allowUnresolvedCycles bool, root *Type, parentStructTypes []*Type) {
	// Most types are encoded in a straightforward fashion
	switch desc := t.Desc.(type) {
	case CycleDesc:
		if allowUnresolvedCycles {
			buf.writeUint8(uint8(desc.Kind()))
			buf.writeUint32(uint32(desc))
		} else {
			panic("found an unexpected unresolved cycle")
		}
	case PrimitiveDesc:
		buf.writeUint8(uint8(desc.Kind()))
	case CompoundDesc:
		switch k := desc.Kind(); k {
		case ListKind, MapKind, RefKind, SetKind:
			buf.writeUint8(uint8(k))
			buf.writeUint32(uint32(len(desc.ElemTypes)))
			for _, tt := range desc.ElemTypes {
				encodeForOID(tt, buf, allowUnresolvedCycles, root, parentStructTypes)
			}
		case UnionKind:
			buf.writeUint8(uint8(k))
			if t == root {
				// If this is where we started we don't need to keep going
				return
			}

			buf.writeUint32(uint32(len(desc.ElemTypes)))

			// This is the only subtle case: encode each subordinate type, generate the hash, remove duplicates, and xor the results together to form an order independent encoding.
			mbuf := newBinaryNomsWriter()
			oids := make(map[hash.Hash]struct{})
			for _, tt := range desc.ElemTypes {
				mbuf.reset()
				encodeForOID(tt, mbuf, allowUnresolvedCycles, root, parentStructTypes)
				oids[hash.FromData(mbuf.data())] = struct{}{}
			}

			data := make([]byte, hash.ByteLen)
			for o := range oids {
				digest := o.Digest()
				for i := 0; i < len(data); i++ {
					data[i] ^= digest[i]
				}
			}
			buf.writeBytes(data)
		default:
			panic("unknown compound type")
		}
	case StructDesc:
		idx, found := indexOfType(t, parentStructTypes)
		if found {
			buf.writeUint8(uint8(CycleKind))
			buf.writeUint32(uint32(len(parentStructTypes)) - 1 - idx)
			return
		}

		buf.writeUint8(uint8(StructKind))
		buf.writeString(desc.Name)

		parentStructTypes = append(parentStructTypes, t)
		for _, field := range desc.fields {
			buf.writeString(field.name)
			encodeForOID(field.t, buf, allowUnresolvedCycles, root, parentStructTypes)
		}
	}
}

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func (tc *TypeCache) makeUnionType(elemTypes ...*Type) *Type {
	seenTypes := map[hash.Hash]bool{}
	ts := flattenUnionTypes(typeSlice(elemTypes), &seenTypes)
	if len(ts) == 1 {
		return ts[0]
	}
	for _, tt := range ts {
		generateOID(tt, true)
	}
	// We sort the contituent types to dedup equivalent types in memory; we may need to sort again after cycles are resolved for final encoding.
	sort.Sort(ts)
	return tc.getCompoundType(UnionKind, ts...)
}

func (tc *TypeCache) getCycleType(level uint32) *Type {
	trie := tc.trieRoots[CycleKind].Traverse(level)

	if trie.t == nil {
		trie.t = newType(CycleDesc(level), tc.nextTypeId())
	}

	return trie.t
}

func flattenUnionTypes(ts typeSlice, seenTypes *map[hash.Hash]bool) typeSlice {
	if len(ts) == 0 {
		return ts
	}

	ts2 := make(typeSlice, 0, len(ts))
	for _, t := range ts {
		if t.Kind() == UnionKind {
			ts2 = append(ts2, flattenUnionTypes(t.Desc.(CompoundDesc).ElemTypes, seenTypes)...)
		} else {
			if !(*seenTypes)[t.Hash()] {
				(*seenTypes)[t.Hash()] = true
				ts2 = append(ts2, t)
			}
		}
	}
	return ts2
}

func MakeListType(elemType *Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.getCompoundType(ListKind, elemType)
}

func MakeSetType(elemType *Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.getCompoundType(SetKind, elemType)
}

func MakeRefType(elemType *Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.getCompoundType(RefKind, elemType)
}

func MakeMapType(keyType, valType *Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.getCompoundType(MapKind, keyType, valType)
}

func MakeStructType(name string, fieldNames []string, fieldTypes []*Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.makeStructType(name, fieldNames, fieldTypes)
}

func MakeUnionType(elemTypes ...*Type) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.makeUnionType(elemTypes...)
}

func MakeCycleType(level uint32) *Type {
	staticTypeCache.Lock()
	defer staticTypeCache.Unlock()
	return staticTypeCache.getCycleType(level)
}

// All types in noms are created in a deterministic order. A typeTrie stores types within a typeCache and allows construction of a prexisting type to return the already existing one rather than allocate a new one.
type typeTrie struct {
	t       *Type
	entries map[uint32]*typeTrie
}

func newTypeTrie() *typeTrie {
	return &typeTrie{entries: map[uint32]*typeTrie{}}
}

func (tct *typeTrie) Traverse(typeId uint32) *typeTrie {
	next, ok := tct.entries[typeId]
	if !ok {
		// Insert edge
		next = newTypeTrie()
		tct.entries[typeId] = next
	}
	return next
}

type identTable struct {
	entries map[string]uint32
	nextId  uint32
}

func newIdentTable() *identTable {
	return &identTable{entries: map[string]uint32{}}
}

func (it *identTable) GetId(ident string) uint32 {
	id, ok := it.entries[ident]
	if !ok {
		id = it.nextId
		it.nextId++
		it.entries[ident] = id
	}

	return id
}
