// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Value from './value.js';
import {Kind} from './noms-kind.js';
import List from './list.js';
import Map from './map.js';
import {getTypeOfValue, StructDesc} from './type.js';

interface Part {
  resolve(v: Promise<?Value>): Promise<?Value>;
  toString(): string;
}

class FieldPart {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  resolve(v: Promise<?Value>): Promise<?Value> {
    return v.then(value => {
      if (value === null || value === undefined) {
        return;
      }

      const t = getTypeOfValue(value);
      if (t.kind !== Kind.Struct) {
        return;
      }

      const f = (t.desc: StructDesc).fields[this.name];
      if (!f) {
        return; // non-present field
      }

      // $FlowIssue: Flow doesn't know that it's safe to just access the field name here.
      return value[this.name];
    });
  }

  toString(): string {
    return `.${this.name}`;
  }
}

// TODO: Support value
type indexType = boolean | number | string;

class IndexPart {
  idx: indexType;

  constructor(idx: indexType) {
    const t = getTypeOfValue(idx);
    switch (t.kind) {
      case Kind.String:
      case Kind.Bool:
      case Kind.Number:
        this.idx = idx;
        break;
      default:
        throw new Error('Unsupported');
    }
  }

  resolve(v: Promise<?Value>): Promise<?Value> {
    return v.then(value => {
      if (value === null || value === undefined) {
        return;
      }

      if (value instanceof List) {
        if (typeof this.idx !== 'number') {
          return;
        }

        if (this.idx < 0 || this.idx >= value.length) {
          return undefined; // index out of bounds
        }

        return value.get(this.idx);
      }

      if (value instanceof Map) {
        return value.get(this.idx);
      }

      return;
    });
  }

  toString(): string {
    switch (typeof this.idx) {
      case 'boolean':
      case 'number':
      case 'string':
        return `[${JSON.stringify(this.idx)}]`;
      default:
        throw new Error('not reached');
    }
  }
}

export default class Path {
  _parts: Array<Part>;

  constructor() {
    this._parts = [];
  }

  _addPart(part: Part): Path {
    const p = new Path();
    p._parts = this._parts.concat(part);
    return p;
  }

  addField(name: string): Path {
    return this._addPart(new FieldPart(name));
  }

  addIndex(idx: indexType): Path {
    return this._addPart(new IndexPart(idx));
  }

  resolve(v: Value): Promise<?Value> {
    return this._parts.reduce((v, p) => p.resolve(v), Promise.resolve(v));
  }

  toString(): string {
    return this._parts.join('');
  }
}
