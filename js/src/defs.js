// @flow

import type {valueOrPrimitive} from './value.js';
import {ValueBase} from './value.js';
import {Type, CompoundDesc, StructDesc} from './type.js';
import type {Field} from './type.js';
import {invariant} from './assert.js';
import {Kind} from './noms-kind.js';
import {newList} from './list.js';
import {newSet} from './set.js';
import {newMap} from './map.js';
import {newBlob} from './blob.js';
import type Struct from './struct.js';
import {newStruct} from './struct.js';

type StructDefType = {[name: string]: DefType};
type DefType = number | string | boolean | Array<DefType> | StructDefType | Uint8Array | ValueBase;

export async function defToNoms(v: DefType, t: Type): Promise<valueOrPrimitive> {
  switch (typeof v) {
    case 'number':
    case 'boolean':
    case 'string':
      return v;
    case 'object':
      break;
    default:
      invariant(false);
  }

  if (v instanceof ValueBase) {
    if (t.equals(v.type)) {
      return v;
    }
  }

  switch (t.kind) {
    case Kind.List: {
      invariant(v instanceof Array);
      invariant(t.desc instanceof CompoundDesc);
      const vt = t.desc.elemTypes[0];
      const vs = await Promise.all(v.map(e => defToNoms(e, vt)));
      return newList(vs, t);
    }

    case Kind.Set: {
      invariant(v instanceof Array);
      invariant(t.desc instanceof CompoundDesc);
      const vt = t.desc.elemTypes[0];
      const vs = await Promise.all(v.map(e => defToNoms(e, vt)));
      return newSet(vs, t);
    }

    case Kind.Map: {
      invariant(v instanceof Array);
      invariant(t.desc instanceof CompoundDesc);
      const ets = t.desc.elemTypes;
      const vs = await Promise.all(v.map((e, i) => defToNoms(e, ets[i % 2])));
      return newMap(vs, t);
    }

    case Kind.Blob:
      invariant(v instanceof Uint8Array);
      return newBlob(v);

    case Kind.Struct: {
      invariant(v instanceof Object);
      return structDefToNoms(v, t);
    }

    default:
      invariant(false, 'unreachable');
  }
}

async function structDefToNoms<T: Struct>(data: StructDefType, type: Type): Promise<T> {
  const {desc} = type;
  invariant(desc instanceof StructDesc);
  const keys = [];
  const ps: Array<Promise<valueOrPrimitive>> = [];
  const add = (f: Field) => {
    const v = data[f.name];
    if (v !== undefined) {
      keys.push(f.name);
      ps.push(defToNoms(v, f.t));
    }
  };
  desc.fields.forEach(add);

  const vals = await Promise.all(ps);
  const newData = Object.create(null);
  for (let i = 0; i < keys.length; i++) {
    newData[keys[i]] = vals[i];
  }
  return newStruct(type, newData);
}
