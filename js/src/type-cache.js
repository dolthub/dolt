// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Hash from './hash.js';
import type {NomsKind} from './noms-kind.js';
import {Kind} from './noms-kind.js';
import {CompoundDesc, CycleDesc, StructDesc, Type} from './type.js';
import {compare} from './compare.js';
import {notNull} from './assert.js';

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
  t: ?Type;
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

  getCompoundType(kind: NomsKind, ...elemTypes: Type[]): Type {
    let trie = notNull(this.trieRoots.get(kind));
    elemTypes.forEach(t => trie = notNull(trie).traverse(t.id));
    if (!notNull(trie).t) {
      trie.t = new Type(new CompoundDesc(kind, elemTypes), this.nextTypeId());
    }

    return notNull(trie.t);
  }

  makeStructType(name: string, fieldNames: string[], fieldTypes: Type[]): Type<StructDesc> {
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
          normalize(t, []);
        }
      }
      t.id = this.nextTypeId();
      trie.t = t;
    }

    return notNull(trie.t);
  }

  // Creates a new union type unless the elemTypes can be folded into a single non union type.
  makeUnionType(types: Type[]): Type {
    types = flattenUnionTypes(types, Object.create(null));
    if (types.length === 1) {
      return types[0];
    }
    types.sort(compare);
    return this.getCompoundType(Kind.Union, ...types);
  }

  getCycleType(level: number): Type {
    const trie = notNull(this.trieRoots.get(Kind.Cycle)).traverse(level);

    if (!trie.t) {
      trie.t = new Type(new CycleDesc(level), this.nextTypeId());
    }

    return notNull(trie.t);
  }
}

export const staticTypeCache = new TypeCache();

function flattenUnionTypes(types: Type[], seenTypes: {[key: Hash]: boolean}): Type[] {
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

const fieldNameRe = /^[a-zA-Z][a-zA-Z0-9_]*$/;

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

function resolveStructCycles(t: Type, parentStructTypes: Type[]): Type {
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
 * Traverses a fully resolved cyclic type and ensures all union types are sorted correctly.
 */
function normalize(t: Type, parentStructTypes: Type[]) {
  const idx = parentStructTypes.indexOf(t);
  if (idx >= 0) {
    return;
  }

  // Note: The JS & Go impls differ here. The Go impl eagerly serializes types as they are
  // constructed. The JS does it lazily so as to avoid cyclic package dependencies.

  const desc = t.desc;
  if (desc instanceof CompoundDesc) {
    for (let i = 0; i < desc.elemTypes.length; i++) {
      normalize(desc.elemTypes[i], parentStructTypes);
    }
    if (t.kind === Kind.Union) {
      desc.elemTypes.sort(compare);
    }

  } else if (desc instanceof StructDesc) {
    for (let i = 0; i < desc.fields.length; i++) {
      parentStructTypes.push(t);
      normalize(desc.fields[i].type, parentStructTypes);
      parentStructTypes.pop();
    }
  }
}

function toUnresolvedType(t: Type, tc: TypeCache, level: number,
                          parentStructTypes: Type[]): [Type, boolean] {
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
