// @flow

import Ref from './ref.js';
import type {valueOrPrimitive} from './value.js';
import {Field, StructDesc, Type} from './type.js';
import {invariant, notNull} from './assert.js';
import {isPrimitive} from './primitives.js';
import {Kind} from'./noms-kind.js';
import {ValueBase} from './value.js';

type StructData = {[key: string]: valueOrPrimitive};

export default class Struct extends ValueBase {
  desc: StructDesc;
  _unionIndex: number;

  _data: StructData;
  typeDef: Type;

  constructor(type: Type, typeDef: Type, data: StructData) {
    super(type);

    invariant(type.kind === Kind.Unresolved);
    invariant(typeDef.kind === Kind.Struct);

    this.typeDef = typeDef;

    const desc = typeDef.desc;
    invariant(desc instanceof StructDesc);
    this.desc = desc;

    this._data = data;
    this._unionIndex = validate(this);
  }

  get chunks(): Array<Ref> {
    const chunks = [];
    chunks.push(...this.type.chunks);
    forEach(this, this._unionField, v => {
      if (!isPrimitive(v)) {
        chunks.push(...v.chunks);
      }
    });
    return chunks;
  }

  get hasUnion(): boolean {
    return this.desc.union.length > 0;
  }

  get unionIndex(): number {
    return this._unionIndex;
  }

  get unionValue(): valueOrPrimitive {
    return this._data[this._unionField.name];
  }

  get _unionField(): Field {
    return this.desc.union[this._unionIndex];
  }

  has(key: string): boolean {
    return this._data[key] !== undefined;
  }

  get(key: string): any {
    return this._data[key];
  }

  set(key: string, value: any): Struct {
    let [f, unionIndex] = findField(this.desc, key); // eslint-disable-line prefer-const
    f = notNull(f);

    const data = Object.create(null);
    this.desc.fields.forEach(f => {
      const v = this._data[f.name];
      if (v !== undefined) {
        data[f.name] = v;
      }
    });

    data[key] = value;
    if (unionIndex === -1 && this.hasUnion) {
      const unionName = this.desc.union[this._unionIndex].name;
      data[unionName] = this._data[unionName];
    }

    return new Struct(this.type, this.typeDef, data);
  }
}

function findField(desc: StructDesc, name: string): [?Field, number] {
  for (let i = 0; i < desc.fields.length; i++) {
    const f = desc.fields[i];
    if (f.name === name) {
      return [f, -1];
    }
  }

  for (let i = 0; i < desc.union.length; i++) {
    const f = desc.union[i];
    if (f.name === name) {
      return [f, i];
    }
  }

  return [null, -1];
}

function validate(s: Struct): number {
  // TODO: Validate field values match field types.
  const data = s._data;
  let dataCount = Object.keys(data).length;
  for (let i = 0; i < s.desc.fields.length; i++) {
    const field = s.desc.fields[i];
    if (data[field.name] !== undefined) {
      dataCount--;
    } else {
      invariant(field.optional);
    }
  }

  if (s.desc.union.length > 0) {
    invariant(dataCount === 1);
    for (let i = 0; i < s.desc.union.length; i++) {
      const field = s.desc.union[i];
      if (data[field.name] !== undefined) {
        return i;
      }
    }

    invariant(false);
  } else {
    invariant(dataCount === 0);
    return -1;
  }
}

function forEach(struct: Struct,
                 unionField: ?Field,
                 callbackfn: (value: any, index: string, field?: Field) => void): void {
  struct.desc.fields.forEach(field => {
    const fieldValue = struct._data[field.name];
    if (fieldValue !== undefined) {
      callbackfn(struct._data[field.name], field.name, field);
    }
  });

  if (unionField) {
    callbackfn(struct._data[unionField.name], unionField.name, unionField);
  }
}
