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
  t: Type;
  entries: Map<number, TypeTrie>;

  constructor() {
    this.entries = new Map();
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

    return trie.t;
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
        t = notNull(this._toUnresolvedType(t, -1));
        t = this._normalize(t);
      }
      t.id = this.nextTypeId();
      trie.t = t;
    }

    return trie.t;
  }

  _toUnresolvedType(t: Type, level: number, parentStructTypes: Type[] = []): ?Type {
    const idx = parentStructTypes.indexOf(t);
    if (idx >= 0) {
      // This type is just a placeholder. It doesn't need an id
      return new Type(new CycleDesc(parentStructTypes.length - idx - 1), 0);
    }

    const desc = t.desc;
    if (desc instanceof CompoundDesc) {
      const elemTypes = desc.elemTypes;
      let sts = elemTypes.map(t => this._toUnresolvedType(t, level, parentStructTypes));
      if (sts.some(t => t)) {
        sts = sts.map((t, i) => t ? t : elemTypes[i]);
        return new Type(new CompoundDesc(t.kind, sts), this.nextTypeId());
      }
      return;
    }

    if (desc instanceof StructDesc) {
      const fields = desc.fields;
      const outerType = t; // TODO: Stupid babel bug.
      const sts = fields.map(f => {
        parentStructTypes.push(outerType);
        const t = this._toUnresolvedType(f.type, level + 1, parentStructTypes);
        parentStructTypes.pop();
        return t ? t : undefined;
      });
      if (sts.some(t => t)) {
        const fs = sts.map((t, i) => ({name: fields[i].name, type: t ? t : fields[i].type}));
        return new Type(new StructDesc(desc.name, fs), this.nextTypeId());
      }
      return;
    }

    if (desc instanceof CycleDesc) {
      const cycleLevel = desc.level;
      return cycleLevel <= level ? t : undefined;
    }
  }

  _normalize(t: Type, parentStructTypes: Type[] = []): Type {
    const desc = t.desc;
    if (desc instanceof CompoundDesc) {
      for (let i = 0; i < desc.elemTypes.length; i++) {
        desc.elemTypes[i] = this._normalize(desc.elemTypes[i], parentStructTypes);
      }
      if (t.kind === Kind.Union) {
        desc.elemTypes.sort(compare);
      }

    } else if (desc instanceof StructDesc) {
      for (let i = 0; i < desc.fields.length; i++) {
        parentStructTypes.push(t);
        desc.fields[i].type = this._normalize(desc.fields[i].type, parentStructTypes);
        parentStructTypes.pop();
      }
    } else if (desc instanceof CycleDesc) {
      const level = desc.level;
      if (level < parentStructTypes.length) {
        return parentStructTypes[parentStructTypes.length - 1 - level];
      }
    }

    return t;
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

    if (trie.t === undefined) {
      trie.t = new Type(new CycleDesc(level), this.nextTypeId());
    }

    return trie.t;
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

