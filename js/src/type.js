// @flow

import Ref from './ref.js';
import type {NomsKind} from './noms_kind.js';
import type {Value} from './value.js';
import {ensureRef} from './get_ref.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';

export type TypeDesc = {
  kind: NomsKind;
  equals: (other: TypeDesc) => boolean;
};

class PrimitiveDesc {
  kind: NomsKind;

  constructor(kind: NomsKind) {
    this.kind = kind;
  }

  equals(other: TypeDesc): boolean {
    return other instanceof PrimitiveDesc && other.kind === this.kind;
  }

  get ordered(): boolean {
    switch (this.kind) {
      case Kind.Float32:
      case Kind.Float64:
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
      case Kind.String:
        return true;
      default:
        return false;
    }
  }
}

class UnresolvedDesc {
  _pkgRef: Ref;
  _ordinal: number;

  constructor(pkgRef: Ref, ordinal: number) {
    this._pkgRef = pkgRef;
    this._ordinal = ordinal;
  }

  get kind(): NomsKind {
    return Kind.Unresolved;
  }

  equals(other: TypeDesc): boolean {
    if (other.kind !== this.kind) {
      return false;
    }
    invariant(other instanceof UnresolvedDesc);

    return other._pkgRef.equals(this._pkgRef) && other._ordinal === this._ordinal;
  }
}

class CompoundDesc {
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

class EnumDesc {
  ids: Array<string>;

  constructor(ids: Array<string>) {
    this.ids = ids;
  }

  get kind(): NomsKind {
    return Kind.Enum;
  }

  equals(other: TypeDesc): boolean {
    if (other.kind !== this.kind) {
      return false;
    }
    invariant(other instanceof EnumDesc);

    if (other.ids.length !== this.ids.length) {
      return false;
    }

    for (let i = 0; i < this.ids.length; i++) {
      if (this.ids[i] !== other.id[i]) {
        return false;
      }
    }

    return true;
  }
}

class StructDesc {
  fields: Array<Field>;
  union: Array<Field>;

  constructor(fields: Array<Field>, union: Array<Field>) {
    this.fields = fields;
    this.union = union;
  }

  get kind(): NomsKind {
    return Kind.Struct;
  }

  equals(other: TypeDesc): boolean {
    if (other.kind !== this.kind) {
      return false;
    }
    invariant(other instanceof StructDesc);

    if (this.fields.length !== other.fields.length || this.union.length !== other.union.length) {
      return false;
    }

    for (let i = 0; i < this.fields.length; i++) {
      if (!this.fields[i].equals(other.fields[i])) {
        return false;
      }
    }

    for (let i = 0; i < this.union.length; i++) {
      if (!this.union[i].equals(other.union[i])) {
        return false;
      }
    }

    return true;
  }
}

class Field {
  name: string;
  t: Type;
  optional: boolean;

  constructor(name: string, t: Type, optional: boolean) {
    this.name = name;
    this.t = t;
    this.optional = optional;
  }

  equals(other: Field): boolean {
    return this.name === other.name && this.t.equals(other.t) && this.optional === other.optional;
  }
}

class Type {
  _namespace: string;
  _name: string;
  _desc: TypeDesc;
  _ref: ?Ref;

  constructor(name: string = '', namespace: string = '', desc: TypeDesc) {
    this._ref = null;
    this._name = name;
    this._namespace = namespace;
    this._desc = desc;
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.type);
  }

  get type(): Type {
    return typeType;
  }

  equals(other: Value): boolean {
    return this.ref.equals(other.ref);
  }

  get chunks(): Array<Ref> {
    const chunks = [];
    if (this.unresolved) {
      if (this.hasPackageRef) {
        chunks.push(this.packageRef);
      }

      return chunks;
    }

    const desc = this._desc;
    if (desc instanceof CompoundDesc) {
      desc.elemTypes.forEach(et => chunks.push(...et.chunks()));
    }

    return chunks;
  }

  get kind(): NomsKind {
    return this._desc.kind;
  }

  get ordered(): boolean {
    const desc = this._desc;
    if (desc instanceof PrimitiveDesc) {
      return desc.ordered;
    }

    return false;
  }

  get desc(): TypeDesc {
    return this._desc;
  }

  get unresolved(): boolean {
    return this._desc instanceof UnresolvedDesc;
  }

  get hasPackageRef(): boolean {
    return this.unresolved && !this.packageRef.isEmpty();
  }

  get packageRef(): Ref {
    invariant(this._desc instanceof UnresolvedDesc);
    return this._desc._pkgRef;
  }

  get ordinal(): number {
    invariant(this._desc instanceof UnresolvedDesc);
    return this._desc._ordinal;
  }

  get name(): string {
    return this._name;
  }

  get namespace(): string {
    return this._namespace;
  }

  get namespacedName(): string {
    let out = '';

    if (this._namespace !== '') {
      out = this._namespace + '.';
    }
    if (this._name !== '') {
      out += this._name;
    }

    return out;
  }

  get elemTypes(): Array<Type> {
    invariant(this._desc instanceof CompoundDesc);
    return this._desc.elemTypes;
  }
}

function buildType(n: string, desc: TypeDesc): Type {
  if (isPrimitiveKind(desc.kind)) {
    return new Type(n, '', desc);
  }

  switch (desc.kind) {
    case Kind.List:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Map:
    case Kind.Enum:
    case Kind.Struct:
    case Kind.Unresolved:
      return new Type(n, '', desc);

    default:
      throw new Error('Unrecognized Kind: ' + desc.kind);
  }
}

function makePrimitiveType(k: NomsKind): Type {
  return buildType('', new PrimitiveDesc(k));
}

function makeCompoundType(k: NomsKind, ...elemTypes: Array<Type>): Type {
  if (elemTypes.length === 1) {
    invariant(k !== Kind.Map, 'Map requires 2 element types');
    invariant(k === Kind.Ref || k === Kind.List || k === Kind.Set);
  } else {
    invariant(k === Kind.Map, 'Only Map can have multiple element types');
    invariant(elemTypes.length === 2, 'Map requires 2 element types');
  }

  return buildType('', new CompoundDesc(k, elemTypes));
}

function makeEnumType(name: string, ids: Array<string>): Type {
  return buildType(name, new EnumDesc(ids));
}

function makeStructType(name: string, fields: Array<Field>, choices: Array<Field>): Type {
  return buildType(name, new StructDesc(fields, choices));
}

function makeType(pkgRef: Ref, ordinal: number): Type {
  return new Type('', '', new UnresolvedDesc(pkgRef, ordinal));
}

function makeUnresolvedType(namespace: string, name: string): Type {
  return new Type(name, namespace, new UnresolvedDesc(new Ref(), -1));
}

export const boolType = makePrimitiveType(Kind.Bool);
export const uint8Type = makePrimitiveType(Kind.Uint8);
export const uint16Type = makePrimitiveType(Kind.Uint16);
export const uint32Type = makePrimitiveType(Kind.Uint32);
export const uint64Type = makePrimitiveType(Kind.Uint64);
export const int8Type = makePrimitiveType(Kind.Int8);
export const int16Type = makePrimitiveType(Kind.Int16);
export const int32Type = makePrimitiveType(Kind.Int32);
export const int64Type = makePrimitiveType(Kind.Int64);
export const float32Type = makePrimitiveType(Kind.Float32);
export const float64Type = makePrimitiveType(Kind.Float64);
export const stringType = makePrimitiveType(Kind.String);
export const typeType = makePrimitiveType(Kind.Type);
export const packageType = makePrimitiveType(Kind.Package);

export {
  CompoundDesc,
  EnumDesc,
  Field,
  makeCompoundType,
  makeEnumType,
  makePrimitiveType,
  makeStructType,
  makeType,
  makeUnresolvedType,
  PrimitiveDesc,
  StructDesc,
  Type,
  UnresolvedDesc,
};
