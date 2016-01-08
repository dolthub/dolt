// @flow

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import {invariant} from './assert.js';
import {packageType, Type} from './type.js';
import {readValue} from './read_value.js';
import {ValueBase} from './value.js';

class Package extends ValueBase {
  types: Array<Type>;
  dependencies: Array<Ref>;

  constructor(types: Array<Type>, dependencies: Array<Ref>) {
    super(packageType);
    this.types = types;
    this.dependencies = dependencies;
  }

  get chunks(): Array<Ref> {
    const chunks = [];
    for (let i = 0; i < this.types.length; i++) {
      chunks.push(...this.types[i].chunks);
    }
    for (let i = 0; i < this.dependencies.length; i++) {
      chunks.push(this.dependencies[i]);
    }
    return chunks;
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
  const refStr = r.toString();
  const p = pendingPackages[refStr];
  if (p) {
    return p;
  }

  return pendingPackages[refStr] = readValue(r, cs).then(p => {
    invariant(p instanceof Package);
    registerPackage(p);
    delete pendingPackages[refStr];
    return p;
  });
}

export {lookupPackage, Package, readPackage, registerPackage};
