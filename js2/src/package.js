/* @flow */

'use strict';

import Ref from './ref.js';
import {Kind} from './noms_kind.js';
import {makePrimitiveTypeRef, TypeRef} from './type_ref.js';
import {encodeNomsValue} from './encode.js';

const packageTypeRef = makePrimitiveTypeRef(Kind.Package);

class Package {
  types: Array<TypeRef>;
  dependencies: Array<Ref>;
  _ref: Ref;

  constructor(types: Array<TypeRef>, dependencies: Array<Ref>) {
    this.types = types;
    this.dependencies = dependencies;
  }

  get ref(): Ref {
    if (!this._ref) {
      this._ref = encodeNomsValue(this, this.typeRef).ref;
    }
    return this._ref;
  }

  get typeRef(): TypeRef {
    return packageTypeRef;
  }
}

const packageRegistry: { [key: string]: Package } = Object.create(null);

function lookupPackage(r: Ref): ?Package {
  return packageRegistry[r.toString()];
}

// TODO: Compute ref rather than setting
function registerPackage(p: Package) {
  packageRegistry[p.ref.toString()] = p;
}

export {lookupPackage, Package, registerPackage};
