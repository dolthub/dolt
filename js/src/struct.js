// @flow

import assertSubtype from './assert-type.js';
import type Ref from './ref.js';
import type {Type, StructDesc} from './type.js';
import type Value from './value.js';
import {Kind} from './noms-kind.js';
import {ValueBase} from './value.js';
import {equals} from './compare.js';
import {getTypeOfValue, makeStructType} from './type.js';
import {invariant} from './assert.js';
import {isPrimitive} from './primitives.js';
import {encode as utf8Encode} from './utf8.js';

type StructData = {[key: string]: Value};

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
export default class Struct extends ValueBase {
  _data: StructData;
  _type: Type;

  constructor(type: Type, data: StructData) {
    super();

    invariant(type.kind === Kind.Struct);

    this._type = type;
    this._data = data;
  }

  get type(): Type {
    return this._type;
  }

  get chunks(): Array<Ref> {
    const mirror = new StructMirror(this);
    const chunks = [];

    const add = field => {
      const {value} = field;
      if (!isPrimitive(value)) {
        invariant(value instanceof ValueBase);
        chunks.push(...value.chunks);
      }
    };

    mirror.forEachField(add);
    return chunks;
  }
}

function validate(type: Type, data: StructData): void {
  type.desc.forEachField((name: string, type: Type) => {
    const value = data[name];
    assertSubtype(type, value);
  });
}

export class StructFieldMirror {
  value: Value;
  name: string;
  type: Type;

  constructor(data: StructData, name: string, type: Type) {
    this.value = data[name];
    this.name = name;
    this.type = type;
  }
}

type FieldCallback = (f: StructFieldMirror) => void;

export class StructMirror<T: Struct> {
  _data: StructData;
  type: Type<StructDesc>;

  constructor(s: Struct) {
    this._data = s._data;
    this.type = s.type;
  }

  get desc(): StructDesc {
    return this.type.desc;
  }

  forEachField(cb: FieldCallback) {
    this.desc.forEachField((name, type) => {
      cb(new StructFieldMirror(this._data, name, type));
    });
  }

  get name(): string {
    return this.type.name;
  }

  get(name: string): ?Value {
    return this._data[name];
  }

  has(name: string): boolean {
    return this.get(name) !== undefined;
  }

  set(name: string, value: ?Value): T {
    const data = addProperty(this, name, value);
    return newStruct(this.name, data);
  }
}

const cache: {[key: string]: Class<any>} = Object.create(null);

function setterName(name) {
  return `set${name[0].toUpperCase()}${name.slice(1)}`;
}

export function createStructClass<T: Struct>(type: Type<StructDesc>): Class<T> {
  const k = type.hash.toString();
  if (cache[k]) {
    return cache[k];
  }

  const c: any = class extends Struct {
    constructor(data: StructData) {
      super(type, data);
    }
  };

  type.desc.forEachField((name: string, _: Type) => {
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
  });

  return cache[k] = c;
}

function getSetter(name: string) {
  return function(value) {
    const newData = Object.assign(Object.create(null), this._data);
    newData[name] = value;
    return new this.constructor(newData);
  };
}

function addProperty(mirror: StructMirror, name: string, value: ?Value): StructData {
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

export function newStruct<T: Struct>(name: string, data: StructData): T {
  return newStructWithTypeNoValidation(computeTypeForStruct(name, data), data);
}

export function newStructWithType<T: Struct>(type: Type<StructDesc>, data: StructData): T {
  validate(type, data);
  return newStructWithTypeNoValidation(type, data);
}

export function newStructWithTypeNoValidation<T: Struct>(type: Type<StructDesc>,
    data: StructData): T {
  return new (createStructClass(type))(data);
}

function computeTypeForStruct(name: string, data: StructData): Type<StructDesc> {
  const keys = Object.keys(data);
  keys.sort();
  const fields = Object.create(null);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    fields[k] = getTypeOfValue(data[k]);
  }
  return makeStructType(name, fields);
}

// s1 & s2 must be of the same type. Returns the set of field names which have different values in
// the respective structs
export function structDiff(s1: Struct, s2: Struct): [string] {
  const desc1: StructDesc = s1.type.desc;
  const desc2: StructDesc = s2.type.desc;
  invariant(desc1.equals(desc2));

  const changed = [];
  desc1.forEachField((name: string, _: Type) => {
    const v1 = s1._data[name];
    const v2 = s2._data[name];
    if (!equals(v1, v2)) {
      changed.push(name);
    }
  });

  return changed;
}

const escapeChar = 'Q';
const headPattern = /[a-zA-PR-Z]/;
const tailPattern = /[a-zA-PR-Z1-9_]/;
const completePattern = new RegExp('^' + headPattern.source + tailPattern.source + '*$');

/**
 * Escapes names for use as noms structs. Disallow characters are encoded as
 * 'Q<hex-encoded-utf8-bytes>'. Note that Q itself is also escaped since it is
 * the escape character.
 */
export function escapeStructField(input: string): string {
  if (completePattern.test(input)) {
    return input;
  }

  if (input.length === 0) {
    throw new Error('cannot escape empty field name');
  }

  const encode = (c: string, p: RegExp) => {
    if (p.test(c) && p !== escapeChar) {
      return c;
    }

    let out = escapeChar;
    utf8Encode(c).forEach(b => {
      const hex = b.toString(16).toUpperCase();
      if (hex.length === 1) {
        out += '0';
      }
      out += hex;
    });
    return out;
  };

  let output = '';
  let pattern = headPattern;
  for (const c of input) {
    output += encode(c, pattern);
    pattern = tailPattern;
  }

  return output;
}
