// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash, {byteLength as hashByteLength} from './hash.js';
import type {NomsKind} from './noms-kind.js';
import {Kind} from './noms-kind.js';
import {CompoundDesc, CycleDesc, PrimitiveDesc, StructDesc, Type} from './type.js';
import {invariant, notNull} from './assert.js';
import {alloc} from './bytes.js';
import {BinaryWriter} from './binary-rw.js';
import {fieldNameRe} from './struct.js';

class IdentTable {
  entries: Map<string, number>;
  nextId: number;

  constructor() {
    this.entries = new Map();
    this.nextId = 0;
  }

  getId(ident: string): number {
    let id = this.entries.get(ident);
    if (id === undefined) {
      id = this.nextId++;
      this.entries.set(ident, id);
    }

    return id;
  }
}

class TypeTrie {
  t: ?Type<any>;
  entries: Map<number, TypeTrie>;

  constructor() {
    this.entries = new Map();
    this.t = undefined;
  }

  traverse(typeId: number): TypeTrie {
    let next = this.entries.get(typeId);
    if (!next) {
      // Insert edge
      next = new TypeTrie();
      this.entries.set(typeId, next);
    }

    return next;
  }
}

export default class TypeCache {
  identTable: IdentTable;
  trieRoots: Map<NomsKind, TypeTrie>;
  nextId: number;

  constructor() {
    this.identTable = new IdentTable();
    this.trieRoots = new Map();
    this.trieRoots.set(Kind.List, new TypeTrie());
    this.trieRoots.set(Kind.Set, new TypeTrie());
    this.trieRoots.set(Kind.Ref, new TypeTrie());
    this.trieRoots.set(Kind.Map, new TypeTrie());
    this.trieRoots.set(Kind.Struct, new TypeTrie());
    this.trieRoots.set(Kind.Cycle, new TypeTrie());
    this.trieRoots.set(Kind.Union, new TypeTrie());
    this.nextId = 256; // The first 255 type ids are reserved for the 8bit space of NomsKinds.
  }

  nextTypeId(): number {
    return this.nextId++;
  }

  getCompoundType(kind: NomsKind, ...elemTypes: Type<any>[]): Type<any> {
    let trie = notNull(this.trieRoots.get(kind));
    elemTypes.forEach(t => trie = notNull(trie).traverse(t.id));
    if (!notNull(trie).t) {
      trie.t = new Type(new CompoundDesc(kind, elemTypes), this.nextTypeId());
    }

    return notNull(trie.t);
  }

  makeStructType(name: string, fields: { [key: string]: Type<any> }): Type<StructDesc> {
    const fieldNames = Object.keys(fields).sort();
    const fieldTypes = fieldNames.map(n => fields[n]);
    return this.makeStructTypeQuickly(name, fieldNames, fieldTypes);
  }

  makeStructTypeQuickly(name: string, fieldNames: Array<string>, fieldTypes: Array<Type<any>>)
      : Type<StructDesc> {
    if (fieldNames.length !== fieldTypes.length) {
      throw new Error('Field names and types must be of equal length');
    }

    verifyStructName(name);
    verifyFieldNames(fieldNames);

    let trie = notNull(this.trieRoots.get(Kind.Struct)).traverse(this.identTable.getId(name));
    fieldNames.forEach((fn, i) => {
      const ft = fieldTypes[i];
      trie = trie.traverse(this.identTable.getId(fn));
      trie = trie.traverse(ft.id);
    });

    if (trie.t === undefined) {
      const fs = fieldNames.map((name, i) => {
        const type = fieldTypes[i];
        return {name, type};
      });

      let t = new Type(new StructDesc(name, fs), 0);
      if (t.hasUnresolvedCycle([])) {
        [t] = toUnresolvedType(t, this, -1, []);
        resolveStructCycles(t, []);
        if (!t.hasUnresolvedCycle([])) {
          normalize(t);
        }
      }
      t.id = this.nextTypeId();
      trie.t = t;
    }

    return notNull(trie.t);
  }

  // Creates a new union type unless the elemTypes can be folded into a single non union type.
  makeUnionType(types: Type<any>[]): Type<any> {
    types = flattenUnionTypes(types, Object.create(null));
    if (types.length === 1) {
      return types[0];
    }
    for (let i = 0; i < types.length; i++) {
      generateOID(types[i], true);
    }
    /*
     * We sort the contituent types to dedup equivalent types in memory; we may need to sort again
     * after cycles are resolved for final encoding.
     */
    types.sort((t1: Type<any>, t2: Type<any>): number => t1.oidCompare(t2));
    return this.getCompoundType(Kind.Union, ...types);
  }

  getCycleType(level: number): Type<any> {
    const trie = notNull(this.trieRoots.get(Kind.Cycle)).traverse(level);

    if (!trie.t) {
      trie.t = new Type(new CycleDesc(level), this.nextTypeId());
    }

    return notNull(trie.t);
  }
}

export const staticTypeCache = new TypeCache();

function flattenUnionTypes(types: Type<any>[], seenTypes: {[key: Hash]: boolean}): Type<any>[] {
  if (types.length === 0) {
    return types;
  }

  const newTypes = [];
  for (let i = 0; i < types.length; i++) {
    if (types[i].kind === Kind.Union) {
      newTypes.push(...flattenUnionTypes(types[i].desc.elemTypes, seenTypes));
    } else {
      if (!seenTypes[types[i].hash]) {
        seenTypes[types[i].hash] = true;
        newTypes.push(types[i]);
      }
    }
  }
  return newTypes;
}

function verifyFieldNames(names: string[]) {
  if (names.length === 0) {
    return;
  }

  let last = names[0];
  verifyFieldName(last);

  for (let i = 1; i < names.length; i++) {
    verifyFieldName(names[i]);
    if (last >= names[i]) {
      throw new Error('Field names must be unique and ordered alphabetically');
    }
    last = names[i];
  }
}

function verifyName(name: string, kind: '' | ' field') {
  if (!fieldNameRe.test(name)) {
    throw new Error(`Invalid struct${kind} name: '${name}'`);
  }
}

function verifyFieldName(name: string) {
  verifyName(name, ' field');
}

function verifyStructName(name: string) {
  if (name !== '') {
    verifyName(name, '');
  }
}

function resolveStructCycles(t: Type<any>, parentStructTypes: Type<any>[]): Type<any> {
  const desc = t.desc;
  if (desc instanceof CompoundDesc) {
    desc.elemTypes.forEach((et, i) => {
      desc.elemTypes[i] = resolveStructCycles(et, parentStructTypes);
    });
  } else if (desc instanceof StructDesc) {
    desc.fields.forEach((f, i) => {
      parentStructTypes.push(t);
      desc.fields[i].type = resolveStructCycles(f.type, parentStructTypes);
      parentStructTypes.pop();
    });
  } else if (desc instanceof CycleDesc) {
    const idx = desc.level;
    if (idx < parentStructTypes.length) {
      return parentStructTypes[parentStructTypes.length - 1 - idx];
    }
  }
  return t;
}

/**
 * We normalize structs during their construction iff they have no unresolved cycles. Normalizing
 * applies a canonical ordering to the composite types of a union (NB: this differs from the Go
 * implementation in that Go also serializes here, but in JS we do it lazily to avoid cylic
 * dependencies). To ensure a consistent ordering of the composite types of a union, we generate
 * a unique "order id" or OID for each of those types. The OID is the hash of a unique type
 * encoding that is independant of the extant order of types within any subordinate unions. This
 * encoding for most types is a straightforward serialization of its components; for unions the
 * encoding is a bytewise XOR of the hashes of each of its composite type encodings.
 *
 * We require a consistent order of types within a union to ensure that equivalent types have a
 * single persistent encoding and, therefore, a single hash. The method described above fails for
 * "unrolled" cycles whereby two equivalent, but uniquely described structures, would have
 * different OIDs.  Consider for example the following two types that, while equivalent, do not
 * yeild the same OID:
 *
 *   Struct A { a: Cycle<0> }
 *   Struct A { a: Struct A { a: Cycle<1> } }
 *
 * We explicitly disallow this sort of redundantly expressed type. If a non-Byzantine use of such a
 * construction arises, we can attempt to simplify the expansive type or find another means of
 * comparison.
 */
function normalize(t: Type<any>) {
  walkType(t, [], (tt: Type<any>) => {
    generateOID(tt, false);
  });

  walkType(t, [], (tt: Type<any>, parentStructTypes: Type<any>[]) => {
    if (tt.kind === Kind.Struct) {
      for (let i = 0; i < parentStructTypes.length; i++) {
        invariant(tt.oidCompare(parentStructTypes[i]) !== 0,
          'unrolled cycle types are not supported; ahl owes you a beer');
      }
    }
  });

  walkType(t, [], (tt: Type<any>) => {
    if (tt.kind === Kind.Union) {
      tt.desc.elemTypes.sort((t1: Type<any>, t2: Type<any>): number => t1.oidCompare(t2));
    }
  });
}

function walkType(t: Type<any>, parentStructTypes: Type<any>[],
    cb: (tt: Type<any>, parents: Type<any>[]) => void) {
  const desc = t.desc;
  if (desc instanceof StructDesc && parentStructTypes.indexOf(t) >= 0) {
    return;
  }

  cb(t, parentStructTypes);

  if (desc instanceof CompoundDesc) {
    for (let i = 0; i < desc.elemTypes.length; i++) {
      walkType(desc.elemTypes[i], parentStructTypes, cb);
    }
  } else if (desc instanceof StructDesc) {
    parentStructTypes.push(t);
    desc.forEachField((_: string, tt: Type<any>) => walkType(tt, parentStructTypes, cb));
    parentStructTypes.pop(t);
  }
}

function generateOID(t: Type<any>, allowUnresolvedCycles: boolean) {
  const buf = new BinaryWriter();
  encodeForOID(t, buf, allowUnresolvedCycles, t, []);
  t.updateOID(Hash.fromData(buf.data));
}

function encodeForOID(t: Type<any>, buf: BinaryWriter, allowUnresolvedCycles: boolean,
    root: Type<any>, parentStructTypes: Type<any>[]) {
  const desc = t.desc;

  if (desc instanceof CycleDesc) {
    invariant(allowUnresolvedCycles, 'found an unexpected resolved cycle');
    buf.writeUint8(desc.kind);
    buf.writeUint32(desc.level);
  } else if (desc instanceof PrimitiveDesc) {
    buf.writeUint8(desc.kind);
  } else if (desc instanceof CompoundDesc) {
    switch (t.kind) {
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        buf.writeUint8(t.kind);
        buf.writeUint32(desc.elemTypes.length);
        for (let i = 0; i < desc.elemTypes.length; i++) {
          encodeForOID(desc.elemTypes[i], buf, allowUnresolvedCycles, root, parentStructTypes);
        }
        break;
      }
      case Kind.Union: {
        buf.writeUint8(t.kind);
        if (t === root) {
          // If this is where we started, we don't need to keep going.
          break;
        }

        buf.writeUint32(desc.elemTypes.length);

        // This is the only subtle case: encode each subordinate type, generate the hash, remove
        // duplicates, and xor the results together to form an order indepedant encoding.
        const mbuf = new BinaryWriter();
        const oids = new Map();
        for (let i = 0; i < t.desc.elemTypes.length; i++) {
          mbuf.reset();
          encodeForOID(t.desc.elemTypes[i], mbuf, allowUnresolvedCycles, root, parentStructTypes);
          const h = Hash.fromData(mbuf.data);
          oids.set(h.toString(), h);
        }

        const data = alloc(hashByteLength);
        oids.forEach((oid: Hash) => {
          const digest = oid.digest;
          for (let i = 0; i < hashByteLength; i++) {
            data[i] ^= digest[i];
          }
        });
        buf.writeBytes(data);
        break;
      }
      default:
        invariant(false, 'unknown compound type');
    }
  } else if (desc instanceof StructDesc) {
    const idx = parentStructTypes.indexOf(t);
    if (idx >= 0) {
      buf.writeUint8(Kind.Cycle);
      buf.writeUint32(parentStructTypes.length - 1 - idx);
      return;
    }

    buf.writeUint8(Kind.Struct);
    buf.writeString(desc.name);

    parentStructTypes.push(t);
    for (let i = 0; i < desc.fields.length; i++) {
      buf.writeString(desc.fields[i].name);
      encodeForOID(desc.fields[i].type, buf, allowUnresolvedCycles, root, parentStructTypes);
    }
    parentStructTypes.pop(t);
  }
}

function toUnresolvedType(t: Type<any>, tc: TypeCache, level: number,
                          parentStructTypes: Type<any>[]): [Type<any>, boolean] {
  const idx = parentStructTypes.indexOf(t);
  if (idx >= 0) {
    // This type is just a placeholder. It doesn't need an id
    return [new Type(new CycleDesc(parentStructTypes.length - idx - 1), 0), true];
  }

  const desc = t.desc;
  if (desc instanceof CompoundDesc) {
    let didChange = false;
    const elemTypes = desc.elemTypes;
    const ts = elemTypes.map(t => {
      const [st, changed] = toUnresolvedType(t, tc, level, parentStructTypes);
      didChange = didChange || changed;
      return st;
    });
    if (!didChange) {
      return [t, false];
    }
    return [new Type(new CompoundDesc(t.kind, ts), tc.nextTypeId()), true];
  }

  if (desc instanceof StructDesc) {
    let didChange = false;
    const fields = desc.fields;
    const outerType = t; // TODO: Stupid babel bug.
    const fs = fields.map(f => {
      const {name} = f;
      parentStructTypes.push(outerType);
      const [type, changed] = toUnresolvedType(f.type, tc, level + 1, parentStructTypes);
      parentStructTypes.pop();
      didChange = didChange || changed;
      return {name, type};
    });
    if (!didChange) {
      return [t, false];
    }
    return [new Type(new StructDesc(desc.name, fs), tc.nextTypeId()), true];
  }

  if (desc instanceof CycleDesc) {
    return [t, desc.level <= level];
  }

  return [t, false];
}
