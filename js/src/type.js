// @flow

import type Ref from './ref.js';
import RefValue from './ref-value.js';
import type {NomsKind} from './noms-kind.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {Value} from './value.js';
import type {valueOrPrimitive} from './value.js';

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
        if (!this.elemTypes[i].equals(other.elemTypes[i])) {
          return false;
        }
      }

      return true;
    }

    return false;
  }
}

function compareFields(a, b) {
  return a.name < b.name ? -1 : 1;  // Cannot be equal.
}

export class StructDesc {
  name: string;
  fields: Array<Field>;

  constructor(name: string, fields: Array<Field>) {
    this.name = name;
    this.fields = fields.sort(compareFields);
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

    if (this.fields.length !== other.fields.length) {
      return false;
    }

    for (let i = 0; i < this.fields.length; i++) {
      if (!this.fields[i].equals(other.fields[i])) {
        return false;
      }
    }

    return true;
  }
}

export class Field {
  name: string;
  type: Type;

  constructor(name: string, type: Type) {
    this.name = name;
    this.type = type;
  }

  equals(other: Field): boolean {
    return this.name === other.name && this.type.equals(other.type);
  }
}

export class Type<T: TypeDesc> extends Value {
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

  get ordered(): boolean {
    switch (this.kind) {
      case Kind.Number:
      case Kind.String:
      case Kind.Ref:
        return true;
      default:
        return false;
    }
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

export function makeCompoundType(k: NomsKind, ...elemTypes: Array<Type>): Type {
  if (elemTypes.length === 1) {
    invariant(k !== Kind.Map, 'Map requires 2 element types');
    invariant(k === Kind.Ref || k === Kind.List || k === Kind.Set);
  } else {
    invariant(k === Kind.Map, 'Only Map can have multiple element types');
    invariant(elemTypes.length === 2, 'Map requires 2 element types');
  }

  return buildType(new CompoundDesc(k, elemTypes));
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

export function makeStructType(name: string, fields: Array<Field>): Type<StructDesc> {
  return buildType(new StructDesc(name, fields));
}

export const boolType = makePrimitiveType(Kind.Bool);
export const numberType = makePrimitiveType(Kind.Number);
export const stringType = makePrimitiveType(Kind.String);
export const blobType = makePrimitiveType(Kind.Blob);
export const typeType = makePrimitiveType(Kind.Type);
export const valueType = makePrimitiveType(Kind.Value);

export const refOfValueType = makeCompoundType(Kind.Ref, valueType);
export const listOfValueType = makeCompoundType(Kind.List, valueType);
export const setOfValueType = makeCompoundType(Kind.Set, valueType);
export const mapOfValueType = makeCompoundType(Kind.Map, valueType, valueType);

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
export function getTypeOfValue(v: valueOrPrimitive): Type {
  if (v instanceof Value) {
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
