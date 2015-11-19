/* @flow */

import Ref from './ref.js';
import {ensureRef} from './get_ref.js';
import {Field, StructDesc, Type} from './type.js';
import {invariant, notNull} from './assert.js';

type StructData = {[key: string]: any};

export default class Struct {
  type: Type;
  desc: StructDesc;
  unionField: ?Field;

  _data: StructData;
  typeDef: Type;
  _ref: Ref;

  constructor(type: Type, typeDef: Type, data: StructData) {
    this.type = type;
    this.typeDef = typeDef;

    let desc = typeDef.desc;
    invariant(desc instanceof StructDesc);
    this.desc = desc;

    this._data = data;
    this.unionField = validate(this);
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.type);
  }

  equals(other: Struct): boolean {
    return this.ref.equals(other.ref);
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
    let [f, isUnion] = findField(this.desc, key);
    f = notNull(f);

    let oldUnionField: ?Field = isUnion && f !== this.unionField ? this.unionField : null;

    let data = Object.create(null);
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
      let fieldValue = this._data[field.name];
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
  for (let f of desc.fields) {
    if (f.name === name) {
      return [f, false];
    }
  }

  for (let f of desc.union) {
    if (f.name === name) {
      return [f, true];
    }
  }

  return [null, false];
}

function validate(s: Struct): ?Field {
  // TODO: Validate field values match field types.
  let data = s._data;
  let dataCount = Object.keys(data).length;
  s.desc.fields.forEach(field => {
    if (data[field.name] !== undefined) {
      dataCount--;
    } else {
      invariant(field.optional);
    }
  });

  if (s.desc.union.length > 0) {
    invariant(dataCount === 1);
    for (let field of s.desc.union) {
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
