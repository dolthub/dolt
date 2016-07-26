// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import assertSubtype from './assert-type.js';
import type Ref from './ref.js';
import type {Type, StructDesc, Field} from './type.js';
import type Value from './value.js';
import {Kind} from './noms-kind.js';
import {ValueBase, init as initValue} from './value.js';
import {equals} from './compare.js';
import {getTypeOfValue, makeStructType, findFieldIndex} from './type.js';
import {invariant} from './assert.js';
import {isPrimitive} from './primitives.js';
import * as Bytes from './bytes.js';

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
 *   get x(): number;
 *   setX(value: number): MyStruct;
 *   get s(): string;
 *   setS(value: string): MyStruct;
 * }
 *
 * To reflect over structs you can create a new StructMirror.
 */
export default class Struct extends ValueBase {
  _type: Type;
  _values: Value[];

  constructor(type: Type<StructDesc>, values: Value[]) {
    super();
    invariant(type.kind === Kind.Struct);
    init(this, type, values);
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

function validate(type: Type, values: Value[]): void {
  let i = 0;
  type.desc.forEachField((name: string, type: Type) => {
    const value = values[i];
    assertSubtype(type, value);
    i++;
  });
}

export class StructFieldMirror {
  value: Value;
  name: string;
  type: Type;

  constructor(value: Value, name: string, type: Type) {
    this.value = value;
    this.name = name;
    this.type = type;
  }
}

type FieldCallback = (f: StructFieldMirror) => void;

export class StructMirror<T: Struct> {
  _values: Value[];
  type: Type<StructDesc>;

  constructor(s: Struct) {
    this._values = s._values;
    this.type = s.type;
  }

  get desc(): StructDesc {
    return this.type.desc;
  }

  forEachField(cb: FieldCallback) {
    this.desc.fields.forEach((f, i) => {
      cb(new StructFieldMirror(this._values[i], f.name, f.type));
    });
  }

  get name(): string {
    return this.desc.name;
  }

  get(name: string): ?Value {
    const i = findFieldIndex(name, this.desc.fields);
    return i !== -1 ? this._values[i] : undefined;
  }

  has(name: string): boolean {
    return findFieldIndex(name, this.desc.fields) !== -1;
  }

  set(name: string, value: ?Value): T {
    const values = setValue(this._values, this.desc.fields, name, value);
    return newStructWithType(this.type, values);
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
      const {fields} = type.desc;
      const values = new Array(fields.length);
      for (let i = 0; i < fields.length; i++) {
        values[i] = data[fields[i].name];
      }

      validate(type, values);
      super(type, values);
    }
  };

  type.desc.fields.forEach((f: Field, i: number) => {
    Object.defineProperty(c.prototype, f.name, {
      configurable: true,
      enumerable: false,
      get: function() {
        return this._values[i];
      },
    });
    Object.defineProperty(c.prototype, setterName(f.name), {
      configurable: true,
      enumerable: false,
      value: getSetter(i),
      writable: true,
    });
  });

  return cache[k] = c;
}

function getSetter(i: number) {
  return function(value) {
    const values = this._values.concat();  // clone
    values[i] = value;
    return newStructWithType(this.type, values);
  };
}

function setValue(values: Value[], fields: Field[], name: string, value: ?Value): Value[] {
  const i = findFieldIndex(name, fields);
  invariant(i !== -1);
  const newValues = values.concat();  // shallow clone
  newValues[i] = value;
  return newValues;
}

export function newStruct<T: Struct>(name: string, data: StructData): T {
  const type = computeTypeForStruct(name, data);
  // Skip validation since there is no way the type and data can mismatch.
  return new (createStructClass(type))(data);
}

export function newStructWithType<T: Struct>(type: Type<StructDesc>, values: Value[]): T {
  validate(type, values);
  return newStructWithValues(type, values);
}

function init<T: Struct>(s: T, type: Type, values: Value[]) {
  s._type = type;
  s._values = values;
}

export function newStructWithValues<T: Struct>(type: Type, values: Value[]): T {
  const c = createStructClass(type);
  const s = Object.create(c.prototype);
  invariant(s instanceof c);
  initValue(s);
  init(s, type, values);
  return s;
}

function computeTypeForStruct(name: string, data: StructData): Type<StructDesc> {
  const fieldNames = Object.keys(data);
  const fieldTypes = new Array(fieldNames.length);
  fieldNames.sort();
  for (let i = 0; i < fieldNames.length; i++) {
    fieldTypes[i] = getTypeOfValue(data[fieldNames[i]]);
  }
  return makeStructType(name, fieldNames, fieldTypes);
}

// s1 & s2 must be of the same type. Returns the set of field names which have different values in
// the respective structs
export function structDiff(s1: Struct, s2: Struct): [string] {
  const desc1: StructDesc = s1.type.desc;
  const desc2: StructDesc = s2.type.desc;
  invariant(desc1.equals(desc2));

  const changed = [];
  desc1.fields.forEach((f: Field, i: number) => {
    const v1 = s1._values[i];
    const v2 = s2._values[i];
    if (!equals(v1, v2)) {
      changed.push(f.name);
    }
  });

  return changed;
}

const escapeChar = 'Q';
const headPattern = /[a-zA-PR-Z]/;
const tailPattern = /[a-zA-PR-Z0-9_]/;
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
    Bytes.fromString(c).forEach(b => {
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
