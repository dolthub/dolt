// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Ref from './ref.js';
import type {NomsKind} from './noms-kind.js';
import type Hash from './hash.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ValueBase} from './value.js';
import type Value from './value.js';
import {describeType} from './encode-human-readable.js';
import search from './binary-search.js';
import {staticTypeCache} from './type-cache.js';

export interface TypeDesc {
  kind: NomsKind;
  hasUnresolvedCycle(visited: Type<any>[]): boolean;
}

export class PrimitiveDesc {
  kind: NomsKind;

  constructor(kind: NomsKind) {
    this.kind = kind;
  }

  hasUnresolvedCycle(visited: Type<any>[]): boolean { // eslint-disable-line no-unused-vars
    return false;
  }
}

export class CompoundDesc {
  kind: NomsKind;
  elemTypes: Array<Type<any>>;

  constructor(kind: NomsKind, elemTypes: Array<Type<any>>) {
    this.kind = kind;
    this.elemTypes = elemTypes;
  }

  hasUnresolvedCycle(visited: Type<any>[]): boolean {
    return this.elemTypes.some(t => t.hasUnresolvedCycle(visited));
  }
}

export type Field = {
  name: string;
  type: Type<any>;
};

export class StructDesc {
  name: string;
  fields: Field[];

  constructor(name: string, fields: Field[]) {
    this.name = name;
    this.fields = fields;
  }

  get fieldCount(): number {
    return this.fields.length;
  }

  get kind(): NomsKind {
    return Kind.Struct;
  }

  hasUnresolvedCycle(visited: Type<any>[]): boolean {
    return this.fields.some(f => f.type.hasUnresolvedCycle(visited));
  }

  forEachField(cb: (name: string, type: Type<any>) => void) {
    const fields = this.fields;
    for (let i = 0; i < fields.length; i++) {
      cb(fields[i].name, fields[i].type);
    }
  }

  getField(name: string): ?Type<any> {
    const f = findField(name, this.fields);
    return f && f.type;
  }
}

function findField(name: string, fields: Field[]): ?Field {
  const i = findFieldIndex(name, fields);
  return i !== -1 ? fields[i] : undefined;
}

/**
 * Finds the index of the `Field` or `-1` if not found.
 */
export function findFieldIndex(name: string, fields: Field[]): number {
  const i = search(fields.length, i => fields[i].name >= name);
  return i === fields.length || fields[i].name !== name ? -1 : i;
}

export class CycleDesc {
  level: number;

  constructor(level: number) {
    this.level = level;
  }

  get kind(): NomsKind {
    return Kind.Cycle;
  }

  hasUnresolvedCycle(visited: Type<any>[]): boolean { // eslint-disable-line no-unused-vars
    return true;
  }
}

/**
 * A Type is created with a descriptor that defines the type and an id. Its OID (order id) is an
 * intermediate hash used to order composite types of a union.
 */
export class Type<T: TypeDesc> extends ValueBase {
  _desc: T;
  _oid: ?Hash;
  id: number;
  serialization: ?Uint8Array;

  constructor(desc: T, id: number) {
    super();
    this._desc = desc;
    this._oid = null;
    this.id = id;
    this.serialization = null;
  }

  get type(): Type<any> {
    return typeType;
  }

  get chunks(): Array<Ref<any>> {
    return [];
  }

  get kind(): NomsKind {
    return this._desc.kind;
  }

  get desc(): T {
    return this._desc;
  }

  updateOID(o: Hash) {
    this._oid = o;
  }

  hasUnresolvedCycle(visited: Type<any>[]): boolean {
    if (visited.indexOf(this) >= 0) {
      return false;
    }

    visited.push(this);
    return this._desc.hasUnresolvedCycle(visited);
  }

  get elemTypes(): Array<Type<any>> {
    invariant(this._desc instanceof CompoundDesc);
    return this._desc.elemTypes;
  }

  oidCompare(other: Type<any>): number {
    return notNull(this._oid).compare(notNull(other._oid));
  }

  describe(): string {
    return describeType(this);
  }
}

function makePrimitiveType(k: NomsKind): Type<PrimitiveDesc> {
  return new Type(new PrimitiveDesc(k), k);
}

export function makeListType(elemType: Type<any>): Type<CompoundDesc> {
  return staticTypeCache.getCompoundType(Kind.List, elemType);
}

export function makeSetType(elemType: Type<any>): Type<CompoundDesc> {
  return staticTypeCache.getCompoundType(Kind.Set, elemType);
}

export function makeMapType(keyType: Type<any>, valueType: Type<any>): Type<CompoundDesc> {
  return staticTypeCache.getCompoundType(Kind.Map, keyType, valueType);
}

export function makeRefType(elemType: Type<any>): Type<CompoundDesc> {
  return staticTypeCache.getCompoundType(Kind.Ref, elemType);
}

export function makeStructType(name: string, fieldNames: string[], fieldTypes: Type<any>[]):
    Type<StructDesc> {
  return staticTypeCache.makeStructType(name, fieldNames, fieldTypes);
}

/**
 * Creates a union type unless the number of distinct types is 1, in which case that type is
 * returned.
 */
export function makeUnionType(types: Type<any>[]): Type<any> {
  return staticTypeCache.makeUnionType(types);
}

export function makeCycleType(level: number): Type<any> {
  return staticTypeCache.getCycleType(level);
}

/**
 * Gives the existing primitive Type value for a NomsKind.
 */
export function getPrimitiveType(k: NomsKind): Type<any> {
  invariant(isPrimitiveKind(k));
  switch (k) {
    case Kind.Bool:
      return boolType;
    case Kind.Number:
      return numberType;
    case Kind.String:
      return stringType;
    case Kind.Blob:
      return blobType;
    case Kind.Type:
      return typeType;
    case Kind.Value:
      return valueType;
    default:
      invariant(false, 'not reachable');
  }
}

// Returns the Noms type of any value. This will throw if you pass in an object that cannot be
// represented by noms.
export function getTypeOfValue(v: Value): Type<any> {
  if (v instanceof ValueBase) {
    return v.type;
  }

  switch (typeof v) {
    case 'string':
      return stringType;
    case 'boolean':
      return boolType;
    case 'number':
      return numberType;
    default:
      throw new Error('Unknown type');
  }
}

export const boolType = makePrimitiveType(Kind.Bool);
export const numberType = makePrimitiveType(Kind.Number);
export const stringType = makePrimitiveType(Kind.String);
export const blobType = makePrimitiveType(Kind.Blob);
export const typeType = makePrimitiveType(Kind.Type);
export const valueType = makePrimitiveType(Kind.Value);
