/* @flow */

'use strict';

import {readValue} from './decode.js';
import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import {encodeNomsValue} from './encode.js';
import {Kind} from './noms_kind.js';
import {makePrimitiveTypeRef, TypeRef} from './type_ref.js';

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

async function readPackage(r: Ref, cs: ChunkStore): Promise<Package> {
  let p = await readValue(r, cs);
  if (p instanceof Package) {
    registerPackage(p);
    return p;
  } else {
    throw new Error('Non-package found where package expected.');
  }
}

export {lookupPackage, Package, readPackage, registerPackage};
