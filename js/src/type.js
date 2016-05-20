// @flow

import type Ref from './ref.js';
import RefValue from './ref-value.js';
import type {NomsKind} from './noms-kind.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ValueBase} from './value.js';
import type Value from './value.js';
import {compare, equals} from './compare.js';

export interface TypeDesc {
  kind: NomsKind;
  equals(other: TypeDesc): boolean;
}

export class PrimitiveDesc {
  kind: NomsKind;

  constructor(kind: NomsKind) {
    this.kind = kind;
  }

  equals(other: TypeDesc): boolean {
    return other instanceof PrimitiveDesc && other.kind === this.kind;
  }
}

export class CompoundDesc {
  kind: NomsKind;
  elemTypes: Array<Type>;

  constructor(kind: NomsKind, elemTypes: Array<Type>) {
    this.kind = kind;
    this.elemTypes = elemTypes;
  }


  equals(other: TypeDesc): boolean {
    if (other instanceof CompoundDesc) {
      if (this.kind !== other.kind || this.elemTypes.length !== other.elemTypes.length) {
        return false;
      }

      for (let i = 0; i < this.elemTypes.length; i++) {
        if (!equals(this.elemTypes[i], other.elemTypes[i])) {
          return false;
        }
      }

      return true;
    }

    return false;
  }
}

export class StructDesc {
  name: string;
  fields: {[key: string]: Type};

  constructor(name: string, fields: {[key: string]: Type}) {
    this.name = name;
    this.fields = fields;
  }

  get kind(): NomsKind {
    return Kind.Struct;
  }

  equals(other: TypeDesc): boolean {
    if (this === other) {
      return true;
    }

    if (other.kind !== this.kind) {
      return false;
    }
    invariant(other instanceof StructDesc);

    const names = Object.keys(this.fields);
    const otherNames = Object.keys(other.fields);

    if (names.length !== otherNames.length) {
      return false;
    }

    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      if (!other.fields[name]) {
        return false;
      }

      if (!equals(this.fields[name], other.fields[name])) {
        return false;
      }
    }

    return true;
  }

  forEachField(cb: (name: string, type: Type) => void) {
    const {fields} = this;
    const names = Object.keys(fields);
    names.sort();
    names.forEach(n => {
      cb(n, fields[n]);
    });
  }
}

export class Type<T: TypeDesc> extends ValueBase {
  _desc: T;
  _ref: ?Ref;

  constructor(desc: T) {
    super();
    this._desc = desc;
  }

  get type(): Type {
    return typeType;
  }

  get chunks(): Array<RefValue> {
    return [];
  }

  get kind(): NomsKind {
    return this._desc.kind;
  }

  get desc(): T {
    return this._desc;
  }

  get name(): string {
    invariant(this._desc instanceof StructDesc);
    return this._desc.name;
  }

  get elemTypes(): Array<Type> {
    invariant(this._desc instanceof CompoundDesc);
    return this._desc.elemTypes;
  }
}

function buildType<T: TypeDesc>(desc: T): Type<T> {
  return new Type(desc);
}

function makePrimitiveType(k: NomsKind): Type<PrimitiveDesc> {
  return buildType(new PrimitiveDesc(k));
}

export function makeListType(elemType: Type): Type<CompoundDesc> {
  return buildType(new CompoundDesc(Kind.List, [elemType]));
}

export function makeSetType(elemType: Type): Type<CompoundDesc> {
  return buildType(new CompoundDesc(Kind.Set, [elemType]));
}

export function makeMapType(keyType: Type, valueType: Type): Type<CompoundDesc> {
  return buildType(new CompoundDesc(Kind.Map, [keyType, valueType]));
}

export function makeRefType(elemType: Type): Type<CompoundDesc> {
  return buildType(new CompoundDesc(Kind.Ref, [elemType]));
}

export function makeStructType(name: string, fields: {[key: string]: Type}): Type<StructDesc> {
  return buildType(new StructDesc(name, fields));
}

/**
 * makeUnionType creates a new union type unless the elemTypes can be folded into a single non
 * union type.
 */
export function makeUnionType(types: Type[]): Type {
  types = flattenUnionTypes(types, Object.create(null));
  if (types.length === 1) {
    return types[0];
  }
  types.sort(compare);
  return buildType(new CompoundDesc(Kind.Union, types));
}

function flattenUnionTypes(types: Type[], seenTypes: {[key: Ref]: boolean}): Type[] {
  if (types.length === 0) {
    return types;
  }

  const newTypes = [];
  for (let i = 0; i < types.length; i++) {
    if (types[i].kind === Kind.Union) {
      newTypes.push(...flattenUnionTypes(types[i].desc.elemTypes, seenTypes));
    } else {
      if (!seenTypes[types[i].ref]) {
        seenTypes[types[i].ref] = true;
        newTypes.push(types[i]);
      }
    }
  }
  return newTypes;
}

export const boolType = makePrimitiveType(Kind.Bool);
export const numberType = makePrimitiveType(Kind.Number);
export const stringType = makePrimitiveType(Kind.String);
export const blobType = makePrimitiveType(Kind.Blob);
export const typeType = makePrimitiveType(Kind.Type);
export const valueType = makePrimitiveType(Kind.Value);

export const refOfBlobType = makeRefType(blobType);
export const refOfValueType = makeRefType(valueType);
export const listOfValueType = makeListType(valueType);
export const setOfValueType = makeSetType(valueType);
export const mapOfValueType = makeMapType(valueType, valueType);

/**
 * Gives the existing primitive Type value for a NomsKind.
 */
export function getPrimitiveType(k: NomsKind): Type {
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
export function getTypeOfValue(v: Value): Type {
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
