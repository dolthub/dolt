/* @flow */

'use strict';

import {assert} from 'chai';
import {suite} from 'mocha';

import test from './async_test.js';
import {JsonArrayWriter} from './encode.js';
import MemoryStore from './memory_store.js';
import {Kind} from './noms_kind.js';
import {makeCompoundTypeRef, makePrimitiveTypeRef} from './type_ref.js';

suite('Encode', () => {
  test('write list', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundTypeRef(Kind.List, makePrimitiveTypeRef(Kind.Int32));
    w.writeTopLevel(tr, [0, 1, 2, 3]);
    assert.deepEqual([Kind.List, Kind.Int32, [0, 1, 2, 3]], w.array);
  });

  test('write list of list', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let it = makeCompoundTypeRef(Kind.List, makePrimitiveTypeRef(Kind.Int16));
    let tr = makeCompoundTypeRef(Kind.List, it);
    let v = [[0], [1, 2, 3]];
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Int16, [[0], [1, 2, 3]]], w.array);
  });

  test('write set', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundTypeRef(Kind.Set, makePrimitiveTypeRef(Kind.UInt32));
    let v = new Set([0, 1, 2, 3]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.UInt32, [1, 3, 0, 2]], w.array);
  });

  test('write set of set', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let st = makeCompoundTypeRef(Kind.Set, makePrimitiveTypeRef(Kind.Int32));
    let tr = makeCompoundTypeRef(Kind.Set, st);
    let v = new Set([new Set([0]), new Set([1, 2, 3])]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Int32, [[1, 3, 2], [0]]], w.array);
  });

  test('write map', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundTypeRef(Kind.Map, makePrimitiveTypeRef(Kind.String), makePrimitiveTypeRef(Kind.Bool));
    let v = new Map();
    v.set('a', false);
    v.set('b', true);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, ['a', false, 'b', true]], w.array);
  });

  test('write map of map', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let kt = makeCompoundTypeRef(Kind.Map, makePrimitiveTypeRef(Kind.String), makePrimitiveTypeRef(Kind.Int64));
    let vt = makeCompoundTypeRef(Kind.Set, makePrimitiveTypeRef(Kind.Bool));
    let tr = makeCompoundTypeRef(Kind.Map, kt, vt);

    let v = new Map();
    let m1 = new Map();
    m1.set('a', 0);
    let s = new Set([true]);
    v.set(m1, s);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.Map, Kind.String, Kind.Int64, Kind.Set, Kind.Bool, [['a', 0], [true]]], w.array);
  });
});
