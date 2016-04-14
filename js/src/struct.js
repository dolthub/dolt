// @flow

import type RefValue from './ref-value.js';
import type {valueOrPrimitive} from './value.js';
import {StructDesc} from './type.js';
import type {Field, Type} from './type.js';
import {invariant} from './assert.js';
import {isPrimitive} from './primitives.js';
import {Kind} from'./noms-kind.js';
import {ValueBase} from './value.js';

type StructData = {[key: string]: ?valueOrPrimitive};

/**
 * Base class for all Noms structs. The decoder creates sub classes of this for Noms struct.
 * These have the form of:
 *
 * ```noms
 * struct MyStruct {
 *   x: Int8
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
  _typeDef: Type;


  constructor(type: Type, typeDef: Type, data: StructData) {
    super();

    invariant(type.kind === Kind.Unresolved);
    invariant(typeDef.kind === Kind.Struct);

    // TODO: Even in dev mode there are paths where the passed in data has already been validated.
    if (process.env.NODE_ENV !== 'production') {
      validate(typeDef, data);
    }

    this._type = type;
    this._typeDef = typeDef;
    this._data = data;
  }

  get type(): Type {
    return this._type;
  }

  get chunks(): Array<RefValue> {
    const mirror = new StructMirror(this);
    const chunks = [];
    chunks.push(...this.type.chunks);

    const add = field => {
      if (!field.present) {
        return;
      }
      const {value} = field;
      if (!isPrimitive(value)) {
        invariant(value instanceof ValueBase);
        chunks.push(...value.chunks);
      }
    };

    mirror.forEachField(add);
    if (mirror.hasUnion) {
      add(mirror.unionField);
    }
    return chunks;
  }
}

function validate(typeDef: Type, data: StructData): void {
  // TODO: Validate field values match field types.
  const {desc} = typeDef;
  invariant(desc instanceof StructDesc);
  const {fields} = desc;
  let dataCount = Object.keys(data).length;
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i];
    invariant(data[field.name] !== undefined || field.optional);
    if (field.name in data) {
      dataCount--;
    }
  }

  const {union} = desc;
  if (union.length > 0) {
    invariant(dataCount === 1);
    for (let i = 0; i < union.length; i++) {
      const field = union[i];
      if (data[field.name] !== undefined) {
        return;
      }
    }

    invariant(false);
  } else {
    invariant(dataCount === 0);
  }
}

export function findUnionIndex(data: StructData, union: Array<Field>): number {
  for (let i = 0; i < union.length; i++) {
    const field = union[i];
    if (data[field.name] !== undefined) {
      return i;
    }
  }
  return -1;
}

export class StructFieldMirror {
  value: ?valueOrPrimitive;
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
  get optional(): boolean {
    return this._f.optional;
  }
  get present(): boolean {
    return this.value !== undefined;
  }
}

type FieldCallback = (f: StructFieldMirror) => void;

export class StructMirror<T: Struct> {
  _data: StructData;
  _type :Type;
  typeDef: Type;

  constructor(s: Struct) {
    this._data = s._data;
    this._type = s.type;
    this.typeDef = s._typeDef;
  }

  get desc(): StructDesc {
    invariant(this.typeDef.desc instanceof StructDesc);
    return this.typeDef.desc;
  }

  forEachField(cb: FieldCallback) {
    this.desc.fields.forEach(field => cb(new StructFieldMirror(this._data, field)));
  }

  get hasUnion(): boolean {
    return this.desc.union.length > 0;
  }

  get unionIndex(): number {
    return findUnionIndex(this._data, this.desc.union);
  }

  get unionField(): StructFieldMirror {
    invariant(this.hasUnion);
    return new StructFieldMirror(this._data, this.desc.union[this.unionIndex]);
  }

  get unionValue(): ?valueOrPrimitive {
    return this._data[this.unionField.name];
  }

  get name(): string {
    return this.typeDef.name;
  }

  get(name: string): ?valueOrPrimitive {
    return this._data[name];
  }

  has(name: string): boolean {
    return this.get(name) !== undefined;
  }

  set(name: string, value: ?valueOrPrimitive): T {
    const data = addProperty(this, name, value);
    return newStruct(this._type, this.typeDef, data);
  }
}

const cache: {[key: string]: Class<any>} = Object.create(null);

function setterName(name) {
  return `set${name[0].toUpperCase()}${name.slice(1)}`;
}

export function createStructClass<T: Struct>(type: Type, typeDef: Type): Class<T> {
  const k = type.ref.toString();
  if (cache[k]) {
    return cache[k];
  }

  const c: any = class extends Struct {
    constructor(data: StructData) {
      super(type, typeDef, data);
    }
  };

  const {desc} = typeDef;
  invariant(desc instanceof StructDesc);

  for (const fields of [desc.fields, desc.union]) {
    const isUnion = fields === desc.union;
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
        value: getSetter(name, field.optional, isUnion),
        writable: true,
      });
    }
  }

  return cache[k] = c;
}

function getSetter(name: string, optional: boolean, union: boolean) {
  if (!optional && !union) {
    return function(value) {
      const newData = Object.assign(Object.create(null), this._data);
      newData[name] = value;
      return new this.constructor(newData);
    };
  }
  if (optional && !union) {
    return function(value) {
      const newData = Object.assign(Object.create(null), this._data);
      if (value === undefined) {
        delete newData[name];
      } else {
        newData[name] = value;
      }
      return new this.constructor(newData);
    };
  }
  return function(value) {
    const data = addProperty(new StructMirror(this), name, value);
    return new this.constructor(data);
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
    } else if (f.present) {
      data[f.name] = f.value;
    }
  });

  if (mirror.hasUnion) {
    if (found) {
      const {unionField} = mirror;
      data[unionField.name] = unionField.value;
    } else {
      const {union} = mirror.desc;
      for (let i = 0; i < union.length; i++) {
        if (union[i].name === name) {
          data[name] = value;
          found = true;
          break;
        }
      }
    }
  }

  invariant(found);

  return data;
}

export function newStruct<T: Struct>(type: Type, typeDef: Type, data: StructData): T {
  const c = createStructClass(type, typeDef);
  return new c(data);
}
