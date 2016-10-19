// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {invariant} from './assert.js';
import type Value from './value.js';
import Hash from './hash.js';
import {Kind} from './noms-kind.js';
import List from './list.js';
import Map from './map.js';
import Set from './set.js';
import {OrderedKey} from './meta-sequence.js';
import {OrderedSequence} from './ordered-sequence.js';
import {fieldNameComponentRe} from './struct.js';
import {getTypeOfValue, StructDesc} from './type.js';

const annotationRe = /^@([a-z]+)/;

/**
 * A single component of a Path.
 */
export interface Part {
  /**
   * Resolves this part in `v`. Returns a Promise to the result, or `null` if unresolved.
   */
  resolve(v: Value): Promise<Value | null>;

  /**
   * Returns the string representation of this Part. It should be parseable back into the Part.
   */
  toString(): string;
}

/**
 * A Path is an address to a Noms value - and unlike hashes (i.e. #abcd...) they can address inlined
 * values.
 *
 * E.g. in a spec like `http://demo.noms.io::foo.bar` this is the `.bar` component.
 *
 * See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
 */
export default class Path {
  _parts: Array<Part>;

  /**
   * Returns `str` parsed as Path. Throws a `SyntaxError` if `str` isn't a valid path.
   */
  static parse(str: string): Path {
    if (str === '') {
      throw new SyntaxError('Empty path');
    }
    const p = new Path();
    constructPath(p._parts, str);
    return p;
  }

  constructor(...parts: Array<Part>) {
    this._parts = parts;
  }

  append(part: Part): Path {
    return new Path(...this._parts.concat(part));
  }

  toString(): string {
    return this._parts.join('');
  }

  async resolve(v: Value): Promise<Value | null> {
    let res = v;
    for (const part of this._parts) {
      if (res === null) {
        break;
      }
      res = await part.resolve(res);
    }
    return res;
  }
}

function constructPath(parts: Array<Part>, str: string) {
  if (str === '') {
    return parts;
  }

  const op = str[0], tail = str.slice(1);

  if (op === '.') {
    const match = tail.match(fieldNameComponentRe);
    if (!match) {
      throw new SyntaxError(`Invalid field: ${tail}`);
    }
    const idx = match[0].length;
    parts.push(new FieldPath(tail.slice(0, idx)));
    constructPath(parts, tail.slice(idx));
    return;
  }

  if (op === '[') {
    if (tail === '') {
      throw new SyntaxError('Path ends in [');
    }

    const [idx, h, rem1] = parsePathIndex(tail);
    const [ann, rem2] = getAnnotation(rem1);

    let rem = rem1;
    let intoKey = false;
    if (ann !== '') {
      if (ann !== 'key') {
        throw new SyntaxError(`Unsupported annotation: @${ann}`);
      }
      intoKey = true;
      rem = rem2;
    }

    let part: Part;
    if (idx !== null) {
      part = new IndexPath(idx, intoKey);
    } else if (h !== null) {
      part = new HashIndexPath(h, intoKey);
    } else {
      throw new Error('unreachable');
    }
    parts.push(part);
    constructPath(parts, rem);
    return;
  }

  if (op === ']') {
    throw new SyntaxError('] is missing opening [');
  }

  throw new SyntaxError(`Invalid operator: ${op}`);
}

function parsePathIndex(str: string): [indexType | null, Hash | null, string] {
  if (str[0] === '"') {
    // String is complicated because ] might be quoted, and " or \ might be escaped.
    const stringBuf = [];
    let i = 1;

    for (; i < str.length; i++) {
      let c = str[i];
      if (c === '"') {
        break;
      }
      if (c === '\\' && i < str.length - 1) {
        i++;
        c = str[i];
        if (c !== '\\' && c !== '"') {
          throw new SyntaxError('Only " and \\ can be escaped');
        }
      }
      stringBuf.push(c);
    }

    if (i === str.length) {
      throw new SyntaxError('[ is missing closing ]');
    }
    return [stringBuf.join(''), null, str.slice(i + 2)];
  }

  const closingIdx = str.indexOf(']');
  if (closingIdx === -1) {
    throw new SyntaxError('[ is missing closing ]');
  }

  const idxStr = str.slice(0, closingIdx);
  const rem = str.slice(closingIdx + 1);

  if (idxStr.length === 0) {
    throw new SyntaxError('Empty index value');
  }

  if (idxStr[0] === '#') {
    const hashStr = idxStr.slice(1);
    const h = Hash.parse(hashStr);
    if (h === null) {
      throw new SyntaxError(`Invalid hash: ${hashStr}`);
    }
    return [null, h, rem];
  }

  if (idxStr === 'true') {
    return [true, null, rem];
  }

  if (idxStr === 'false') {
    return [false, null, rem];
  }

  const n = Number(idxStr);
  if (!Number.isNaN(n)) {
    return [n, null, rem];
  }

  throw new SyntaxError(`Invalid index: ${idxStr}`);
}

function getAnnotation(str: string): [string /* ann */, string /* rem */] {
  const parts = annotationRe.exec(str);
  if (parts) {
    invariant(parts.length === 2);
    return [parts[1], str.slice(parts[0].length)];
  }
  return ['', str];
}

/**
 * Gets Struct field values by name.
 */
export class FieldPath {
  /**
   * The name of the field, e.g. `.Name`.
   */
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  async resolve(value: Value): Promise<Value | null> {
    const t = getTypeOfValue(value);
    if (t.kind !== Kind.Struct) {
      return null;
    }

    const f = (t.desc: StructDesc).getField(this.name);
    if (!f) {
      return null; // non-present field
    }

    // $FlowIssue: Flow doesn't know that it's safe to just access the field name here.
    return valueOrNull(value[this.name]);
  }

  toString(): string {
    return `.${this.name}`;
  }
}

// TODO: Support value
type indexType = boolean | number | string;

/**
 * Indexes into Maps and Lists by key or index.
 */
export class IndexPath {
  /**
   * The value of the index, e.g. `[42]` or `["value"]`.
   */
  index: indexType;

  /**
   * Whether this index should resolve to the key of a map, given by a `@key` annotation.
   *
   * Typically IntoKey is false, and indices would resolve to the values. E.g.  given `{a: 42}`
   * then `["a"]` resolves to `42`.
   *
   * If IntoKey is true, then it resolves to `"a"`. For IndexPath this isn't particularly useful
   * - it's mostly provided for consistency with HashIndexPath - but note that given `{a: 42}`
   *   then `["b"]` resolves to null, not `"b"`.
   */
  intoKey: boolean;

  constructor(idx: indexType, intoKey: boolean = false) {
    const t = getTypeOfValue(idx);
    switch (t.kind) {
      case Kind.String:
      case Kind.Bool:
      case Kind.Number:
        this.index = idx;
        break;
      default:
        throw new Error('Unsupported');
    }
    this.intoKey = intoKey;
  }

  async resolve(value: Value): Promise<Value | null> {
    if (value instanceof List) {
      if (typeof this.index !== 'number') {
        return null;
      }
      if (this.index < 0 || this.index >= value.length) {
        return null; // index out of bounds
      }
      return this.intoKey ? this.index : value.get(this.index).then(valueOrNull);
    }

    if (value instanceof Map) {
      if (this.intoKey && await value.has(this.index)) {
        return this.index;
      }
      if (!this.intoKey) {
        return value.get(this.index).then(valueOrNull);
      }
    }

    return null;
  }

  toString(): string {
    const ann = this.intoKey ? '@key' : '';
    switch (typeof this.index) {
      case 'boolean':
      case 'number':
      case 'string':
        return `[${JSON.stringify(this.index)}]${ann}`;
      default:
        throw new Error('not reached');
    }
  }
}

/**
 * Indexes into Maps by the hash of a key, or a Set by the hash of a value.
 */
export class HashIndexPath {
  /**
   * The hash of the key or value to search for. Maps and Set are ordered, so this in
   * O(log(size)).
   */
  hash: Hash;

  /**
   * Whether this index should resolve to the key of a map, given by a `@key` annotation.
   *
   * Typically IntoKey is false, and indices would resolve to the values. E.g. given `{a: 42}`
   * and if the hash of `"a"` is `#abcd`, then `[#abcd]` resolves to `42`.
   *
   * If IntoKey is true, then it resolves to `"a"`. This is useful for when Map keys aren't
   * primitive values, e.g. a struct, since struct literals can't be spelled using a Path.
   */
  intoKey: boolean;

  constructor(h: Hash, intoKey: boolean = false) {
    invariant(!h.isEmpty());
    this.hash = h;
    this.intoKey = intoKey;
  }

  async resolve(value: Value): Promise<Value | null> {
    let seq: OrderedSequence<any, any>;
    let getCurrentValue; // (cur: sequenceCursor): Value

    if (value instanceof Set) {
      // Unclear what the behavior should be if |this.intoKey| is true, but ignoring it for
      // sets is arguably correct.
      seq = value.sequence;
      getCurrentValue = cur => cur.getCurrent();
    } else if (value instanceof Map) {
      seq = value.sequence;
      if (this.intoKey) {
        getCurrentValue = cur => cur.getCurrent()[0]; // key
      } else {
        getCurrentValue = cur => cur.getCurrent()[1]; // value
      }
    } else {
      return null;
    }

    const cur = await seq.newCursorAt(OrderedKey.fromHash(this.hash));
    if (!cur.valid) {
      return null;
    }

    const currentHash = cur.getCurrentKey().h;
    if (!currentHash || !currentHash.equals(this.hash)) {
      return null;
    }

    return getCurrentValue(cur);
  }

  toString(): string {
    const ann = this.intoKey ? '@key' : '';
    return `[#${this.hash.toString()}]${ann}`;
  }
}

function valueOrNull(v: ?Value): Value | null {
  return v === undefined ? null : v;
}
