// @flow

import {NomsBlob} from './blob.js';
import {NomsList} from './list.js';
import {NomsSet} from './set.js';
import {NomsMap} from './map.js';
import RefValue from './ref-value.js';
import Struct, {StructMirror} from './struct.js';

import type Database from './database.js';
import type {valueOrPrimitive} from './value.js';

// Invokes |cb| once for |v| and each of its descendants. The returned promise is resolved when all
// invocations to |cb| have been resolved.
//
// The return value of |cb| indicates whether to recurse further into the tree. Return false to
// skip a node's children.
//
// For convenience, if |cb| returns |undefined|, the default is |true|.
export default async function walk(v: valueOrPrimitive, ds: Database,
                                   cb: (v: valueOrPrimitive) => Promise<?bool>): Promise<void> {
  let cont = await cb(v);
  if (cont === undefined) {
    cont = true;
  }
  if (!cont) {
    return;
  }

  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return;
  }

  if (v instanceof NomsBlob) {
    return;
  }

  if (v instanceof RefValue) {
    return walk(await v.targetValue(ds), ds, cb);
  }

  const p = [];
  if (v instanceof NomsList || v instanceof NomsSet) {
    await v.forEach(cv => void(p.push(walk(cv, ds, cb))));
  } else if (v instanceof NomsMap) {
    await v.forEach((cv, k) => {
      p.push(walk(k, ds, cb));
      p.push(walk(cv, ds, cb));
    });
  } else if (v instanceof Struct) {
    new StructMirror(v).forEachField(f => {
      p.push(walk(f.value, ds, cb));
    });
  } else {
    throw new Error('not reached');
  }

  return Promise.all(p).then();
}
