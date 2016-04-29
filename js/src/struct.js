// @flow

import type RefValue from './ref-value.js';
import type {valueOrPrimitive} from './value.js';
import {StructDesc} from './type.js';
import type {Field, Type} from './type.js';
import {invariant} from './assert.js';
import {isPrimitive} from './primitives.js';
import {Kind} from './noms-kind.js';
import {Value} from './value.js';
import validateType from './validate-type.js';

type StructData = {[key: string]: valueOrPrimitive};

/**
 * Base class for all Noms structs. The decoder creates sub classes of this for Noms struct.
 * These have the form of:
 *
 * ```noms
 * struct MyStruct {
 *   x: Number
 *   s: string
 * }
 * ```
 *
 * ```js
 * interface MyStruct extends Struct {
 *   get x(): int8;
 *   setX(value: int8): MyStruct;
 *   get s(): string;
 *   setS(value: string): MyStruct;
 * }
 *
 * To reflect over structs you can create a new StructMirror.
 */
export default class Struct extends Value {
  _data: StructData;
  _type: Type;

  constructor(type: Type, data: StructData) {
    super();

    invariant(type.kind === Kind.Struct);

    // TODO: Even in dev mode there are paths where the passed in data has already been validated.
    if (process.env.NODE_ENV !== 'production') {
      validate(type, data);
    }

    this._type = type;
    this._data = data;
  }

  get type(): Type {
    return this._type;
  }

  get chunks(): Array<RefValue> {
    const mirror = new StructMirror(this);
    const chunks = [];

    const add = field => {
      const {value} = field;
      if (!isPrimitive(value)) {
        invariant(value instanceof Value);
        chunks.push(...value.chunks);
      }
    };

    mirror.forEachField(add);
    return chunks;
  }
}

function validate(type: Type, data: StructData): void {
  // TODO: Validate field values match field types.
  const {desc} = type;
  invariant(desc instanceof StructDesc);
  const {fields} = desc;
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i];
    const value = data[field.name];
    validateType(field.t, value);
  }
}

export class StructFieldMirror {
  value: valueOrPrimitive;
  _f: Field;

  constructor(data: StructData, f: Field) {
    this.value = data[f.name];
    this._f = f;
  }
  get name(): string {
    return this._f.name;
  }
  get type(): Type {
    return this._f.t;
  }
}

type FieldCallback = (f: StructFieldMirror) => void;

export class StructMirror<T: Struct> {
  _data: StructData;
  type :Type;

  constructor(s: Struct) {
    this._data = s._data;
    this.type = s.type;
  }

  get desc(): StructDesc {
    invariant(this.type.desc instanceof StructDesc);
    return this.type.desc;
  }

  forEachField(cb: FieldCallback) {
    this.desc.fields.forEach(field => cb(new StructFieldMirror(this._data, field)));
  }

  get name(): string {
    return this.type.name;
  }

  get(name: string): ?valueOrPrimitive {
    return this._data[name];
  }

  has(name: string): boolean {
    return this.get(name) !== undefined;
  }

  set(name: string, value: ?valueOrPrimitive): T {
    const data = addProperty(this, name, value);
    return newStruct(this.type, data);
  }
}

const cache: {[key: string]: Class<any>} = Object.create(null);

function setterName(name) {
  return `set${name[0].toUpperCase()}${name.slice(1)}`;
}

export function createStructClass<T: Struct>(type: Type): Class<T> {
  const k = type.ref.toString();
  if (cache[k]) {
    return cache[k];
  }

  const c: any = class extends Struct {
    constructor(data: StructData) {
      super(type, data);
    }
  };

  const {desc} = type;
  invariant(desc instanceof StructDesc);

  const {fields} = desc;
  for (const field of fields) {
    const {name} = field;
    Object.defineProperty(c.prototype, name, {
      configurable: true,
      enumerable: false,
      get: function() {
        return this._data[name];
      },
    });
    Object.defineProperty(c.prototype, setterName(name), {
      configurable: true,
      enumerable: false,
      value: getSetter(name),
      writable: true,
    });
  }

  return cache[k] = c;
}

function getSetter(name: string) {
  return function(value) {
    const newData = Object.assign(Object.create(null), this._data);
    newData[name] = value;
    return new this.constructor(newData);
  };
}

function addProperty(mirror: StructMirror, name: string, value: ?valueOrPrimitive): StructData {
  const data = Object.create(null);
  let found = false;
  mirror.forEachField(f => {
    if (f.name === name) {
      if (value !== undefined) {
        data[name] = value;
      }
      found = true;
    } else {
      data[f.name] = f.value;
    }
  });

  invariant(found);
  return data;
}

export function newStruct<T: Struct>(type: Type, data: StructData): T {
  const c = createStructClass(type);
  return new c(data);
}
