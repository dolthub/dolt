// @flow

import Ref from './ref.js';
import {invariant} from './assert.js';
import {packageType, Type} from './type.js';
import {ValueBase} from './value.js';
import type {DataStore} from './data-store.js';

export class Package extends ValueBase {
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

export function lookupPackage(r: Ref): ?Package {
  return packageRegistry[r.toString()];
}

// TODO: Compute ref rather than setting
export function registerPackage(p: Package) {
  packageRegistry[p.ref.toString()] = p;
}

const pendingPackages: { [key: string]: Promise<Package> } = Object.create(null);

export function readPackage(r: Ref, ds: DataStore): Promise<Package> {
  const refStr = r.toString();
  const p = pendingPackages[refStr];
  if (p) {
    return p;
  }

  return pendingPackages[refStr] = ds.readValue(r).then(p => {
    invariant(p instanceof Package);
    registerPackage(p);
    delete pendingPackages[refStr];
    return p;
  });
}
