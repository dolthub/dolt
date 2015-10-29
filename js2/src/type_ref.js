/* @flow */

'use strict';

import {isPrimitiveKind, Kind} from './noms_kind.js';
import Ref from './ref.js';
import type {NomsKind} from './noms_kind.js';

type TypeDesc = {
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
    return other instanceof UnresolvedDesc && other._pkgRef.equals(this._pkgRef)
        && other._ordinal === this._ordinal;
  }
}

class CompoundDesc {
  kind: NomsKind;
  elemTypes: Array<TypeRef>;

  constructor(kind: NomsKind, elemTypes: Array<TypeRef>) {
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
    if (other instanceof StructDesc) {
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

    return false;
  }
}

class Field {
  name: string;
  t: TypeRef;
  optional: boolean;

  constructor(name: string, t: TypeRef, optional: boolean) {
    this.name = name;
    this.t = t;
    this.optional = optional;
  }

  equals(other: Field): boolean {
    return this.name === other.name && this.t.equals(other.t) && this.optional === other.optional;
  }
}


class TypeRef {
  _namespace: string;
  _name: string;
  _desc: TypeDesc;
  _ref: Ref;

  constructor(name: string = '', namespace: string = '', desc: TypeDesc, ref: Ref = new Ref()) {
    this._name = name;
    this._namespace = namespace;
    this._desc = desc;
    this._ref = ref;
  }

  equals(other: TypeRef): boolean {
    // TODO: Go code uses Ref() equality.
    return this.namespacedName === other.namespacedName && this._desc.equals(other._desc);
  }

  get kind(): NomsKind {
    return this._desc.kind;
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
    if (this._desc instanceof UnresolvedDesc) {
      return this._desc._pkgRef;
    }

    throw new Error('PackageRef only works on unresolved type refs');
  }

  get ordinal(): number {
    if (this._desc instanceof UnresolvedDesc) {
      return this._desc._ordinal;
    }

    throw new Error('Ordinal has not been set');
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

  get elemTypes(): Array<TypeRef> {
    if (this._desc instanceof CompoundDesc) {
      return this._desc.elemTypes;
    }

    throw new Error('Only CompoundDesc have elemTypes');
  }
}

function buildType(n: string, desc: TypeDesc): TypeRef {
  if (isPrimitiveKind(desc.kind)) {
    return new TypeRef(n, '', desc);
  }

  switch (desc.kind) {
    case Kind.List:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Map:
    case Kind.Enum:
    case Kind.Struct:
    case Kind.Unresolved:
      return new TypeRef(n, '', desc);

    default:
      throw new Error('Unrecognized Kind: ' + desc.kind);
  }
}

function makePrimitiveTypeRef(k: NomsKind): TypeRef {
  return buildType('', new PrimitiveDesc(k));
}

function makeCompoundTypeRef(k: NomsKind, ...elemTypes: Array<TypeRef>): TypeRef {
  if (elemTypes.length === 1) {
    if (k === Kind.Map) {
      throw new Error('Map requires 2 element types');
    }
  } else {
    if (k !== Kind.Map) {
      throw new Error('Only Map can have multiple element types');
    }
    if (elemTypes.length !== 2) {
      throw new Error('Map requires 2 element types');
    }
  }

  return buildType('', new CompoundDesc(k, elemTypes));
}

function makeStructTypeRef(name: string, fields: Array<Field>, choices: Array<Field>): TypeRef {
  return buildType(name, new StructDesc(fields, choices));
}

function makeTypeRef(pkgRef: Ref, ordinal: number): TypeRef {
  return new TypeRef('', '', new UnresolvedDesc(pkgRef, ordinal));
}

export {CompoundDesc, Field, makeCompoundTypeRef, makePrimitiveTypeRef, makeStructTypeRef, makeTypeRef, StructDesc, TypeRef};
