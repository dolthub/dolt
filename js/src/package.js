// @flow

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import {ensureRef} from './get_ref.js';
import {invariant} from './assert.js';
import {packageType, Type} from './type.js';
import {readValue} from './read_value.js';

class Package {
  types: Array<Type>;
  dependencies: Array<Ref>;
  _ref: Ref;

  constructor(types: Array<Type>, dependencies: Array<Ref>) {
    this.types = types;
    this.dependencies = dependencies;
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.type);
  }

  get type(): Type {
    return packageType;
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

const pendingPackages: { [key: string]: Promise<Package> } = Object.create(null);

function readPackage(r: Ref, cs: ChunkStore): Promise<Package> {
  let refStr = r.toString();
  let p = pendingPackages[refStr];
  if (p) {
    return p;
  }

  return pendingPackages[refStr] = readValue(r, cs).then((p: Package) => {
    invariant(p instanceof Package);
    registerPackage(p);
    delete pendingPackages[refStr];
    return p;
  });
}

export {lookupPackage, Package, readPackage, registerPackage};
