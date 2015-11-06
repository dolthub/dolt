/* @flow */

'use strict';

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import test from './async_test.js';
import {Field, makeCompoundTypeRef, makePrimitiveTypeRef, makeStructTypeRef, makeTypeRef} from './type_ref.js';
import {JsonArrayWriter} from './encode.js';
import {Kind} from './noms_kind.js';
import {Package, registerPackage} from './package.js';

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

  test('write empty struct', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructTypeRef('S', [], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typeRef = makeTypeRef(pkgRef, 0);

    let v = new Struct(typeRef, typeDef, {});

    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0], w.array);
  });

  test('write struct', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructTypeRef('S', [
      new Field('x', makePrimitiveTypeRef(Kind.Int8), false),
      new Field('b', makePrimitiveTypeRef(Kind.Bool), false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typeRef = makeTypeRef(pkgRef, 0);

    let v = new Struct(typeRef, typeDef, {x: 42, b: true});

    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, 42, true], w.array);
  });

  test('write struct optional field', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructTypeRef('S', [
      new Field('x', makePrimitiveTypeRef(Kind.Int8), true),
      new Field('b', makePrimitiveTypeRef(Kind.Bool), false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typeRef = makeTypeRef(pkgRef, 0);

    let v = new Struct(typeRef, typeDef, {x: 42, b: true});
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, true, 42, true], w.array);

    v = new Struct(typeRef, typeDef, {b: true});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, false, true], w.array);
  });

  test('write struct with union', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructTypeRef('S', [
      new Field('x', makePrimitiveTypeRef(Kind.Int8), false)
    ], [
      new Field('b', makePrimitiveTypeRef(Kind.Bool), false),
      new Field('s', makePrimitiveTypeRef(Kind.String), false)
    ]);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typeRef = makeTypeRef(pkgRef, 0);

    let v = new Struct(typeRef, typeDef, {x: 42, s: 'hi'});
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, 42, 1, 'hi'], w.array);

    v = new Struct(typeRef, typeDef, {x: 42, b: true});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, 42, 0, true], w.array);
  });

  test('write struct with list', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructTypeRef('S', [
      new Field('l', makeCompoundTypeRef(Kind.List, makePrimitiveTypeRef(Kind.String)), false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typeRef = makeTypeRef(pkgRef, 0);

    let v = new Struct(typeRef, typeDef, {l: ['a', 'b']});
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, ['a', 'b']], w.array);

    v = new Struct(typeRef, typeDef, {l: []});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(typeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 0, []], w.array);
  });

  test('write struct with struct', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let s2TypeDef = makeStructTypeRef('S2', [
      new Field('x', makePrimitiveTypeRef(Kind.Int32), false)
    ], []);
    let sTypeDef = makeStructTypeRef('S', [
      new Field('s', makeTypeRef(new Ref(), 0), false)
    ], []);

    let pkg = new Package([s2TypeDef, sTypeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let s2TypeRef = makeTypeRef(pkgRef, 0);
    let sTypeRef = makeTypeRef(pkgRef, 1);

    let v = new Struct(sTypeRef, sTypeDef, {s: new Struct(s2TypeRef, s2TypeDef, {x: 42})});
    w.writeTopLevel(sTypeRef, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), 1, 42], w.array);
  });
});
