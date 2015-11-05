/* @flow */

'use strict';

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import {ensureRef} from './get_ref.js';
import {invariant} from './assert.js';
import {packageTypeRef, TypeRef} from './type_ref.js';
import {readValue} from './decode.js';

class Package {
  types: Array<TypeRef>;
  dependencies: Array<Ref>;
  _ref: Ref;

  constructor(types: Array<TypeRef>, dependencies: Array<Ref>) {
    this.types = types;
    this.dependencies = dependencies;
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.typeRef);
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
  invariant(p instanceof Package);
  registerPackage(p);
  return p;
}

export {lookupPackage, Package, readPackage, registerPackage};
