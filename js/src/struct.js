// @flow

import Ref from './ref.js';
import {Field, StructDesc, Type} from './type.js';
import {invariant, notNull} from './assert.js';
import {isPrimitive} from './primitives.js';
import {ValueBase} from './value.js';

type StructData = {[key: string]: any};

export default class Struct extends ValueBase {
  desc: StructDesc;
  unionField: ?Field;

  _data: StructData;
  typeDef: Type;

  constructor(type: Type, typeDef: Type, data: StructData) {
    super(type);

    this.typeDef = typeDef;

    const desc = typeDef.desc;
    invariant(desc instanceof StructDesc);
    this.desc = desc;

    this._data = data;
    this.unionField = validate(this);
  }

  get chunks(): Array<Ref> {
    const chunks = [];
    chunks.push(...this.type.chunks);
    this.forEach(v => {
      if (!isPrimitive(v)) {
        chunks.push(...v.chunks);
      }
    });
    return chunks;
  }

  get fields(): Array<Field> {
    return this.desc.fields;
  }

  get hasUnion(): boolean {
    return this.desc.union.length > 0;
  }

  get unionIndex(): number {
    return this.desc.union.indexOf(notNull(this.unionField));
  }

  has(key: string): boolean {
    return this._data[key] !== undefined;
  }

  get(key: string): any {
    return this._data[key];
  }

  set(key: string, value: any): Struct {
    let [f, isUnion] = findField(this.desc, key); // eslint-disable-line prefer-const
    f = notNull(f);

    const oldUnionField: ?Field = isUnion && f !== this.unionField ? this.unionField : null;

    const data = Object.create(null);
    Object.keys(this._data).forEach(f => {
      if (!oldUnionField || oldUnionField.name !== f) {
        data[f] = this._data[f];
      }
    });

    data[key] = value;
    return new Struct(this.type, this.typeDef, data);
  }

  forEach(callbackfn: (value: any, index: string, field?: Field) => void): void {
    this.desc.fields.forEach(field => {
      const fieldValue = this._data[field.name];
      if (fieldValue !== undefined) {
        callbackfn(this._data[field.name], field.name, field);
      }
    });

    if (this.unionField) {
      callbackfn(this._data[this.unionField.name], this.unionField.name, this.unionField);
    }
  }
}

function findField(desc: StructDesc, name: string): [?Field, boolean] {
  for (let i = 0; i < desc.fields.length; i++) {
    const f = desc.fields[i];
    if (f.name === name) {
      return [f, false];
    }
  }

  for (let i = 0; i < desc.union.length; i++) {
    const f = desc.union[i];
    if (f.name === name) {
      return [f, true];
    }
  }

  return [null, false];
}

function validate(s: Struct): ?Field {
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
        return field;
      }
    }

    invariant(false);
  } else {
    invariant(dataCount === 0);
    return null;
  }
}
