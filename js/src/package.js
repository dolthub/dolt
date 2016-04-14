// @flow

import type Ref from './ref.js';
import RefValue from './ref-value.js';
import {invariant} from './assert.js';
import type {Type} from './type.js';
import {packageType, packageRefType} from './type.js';
import {ValueBase} from './value.js';
import type DataStore from './data-store.js';
import {getRef} from './get-ref.js';
import fixupType from './fixup-type.js';

export class Package extends ValueBase {
  types: Array<Type>;
  dependencies: Array<Ref>;
  _ref: Ref;

  constructor(types: Array<Type>, dependencies: Array<Ref>) {
    super();
    this.types = types;
    this.dependencies = dependencies;
    this._ref = getRef(this, this.type);
    this.types = types.map(t => fixupType(t, this));
  }

  get ref(): Ref {
    return this._ref;
  }

  get type(): Type {
    return packageType;
  }

  get chunks(): Array<RefValue> {
    const chunks = [];
    for (let i = 0; i < this.types.length; i++) {
      chunks.push(...this.types[i].chunks);
    }
    for (let i = 0; i < this.dependencies.length; i++) {
      chunks.push(new RefValue(this.dependencies[i], packageRefType));
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
