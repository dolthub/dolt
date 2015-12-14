// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import test from './async_test.js';
import type {NomsKind} from './noms_kind.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, Type} from './type.js';
import {JsonArrayWriter, encodeNomsValue} from './encode.js';
import {Kind} from './noms_kind.js';
import {ListLeaf, CompoundList} from './list.js';
import {MapLeaf} from './map.js';
import {MetaTuple} from './meta_sequence.js';
import {Package, registerPackage} from './package.js';
import {SetLeaf} from './set.js';
import {writeValue} from './encode.js';

suite('Encode', () => {
  test('write primitives', () => {
    function f(k: NomsKind, v: any, ex: any) {
      let ms = new MemoryStore();
      let w = new JsonArrayWriter(ms);
      w.writeTopLevel(makePrimitiveType(k), v);
      assert.deepEqual([k, ex], w.array);
    }

    f(Kind.Bool, true, true);
    f(Kind.Bool, false, false);

    f(Kind.Uint8, 0, '0');
    f(Kind.Uint16, 0, '0');
    f(Kind.Uint32, 0, '0');
    f(Kind.Uint64, 0, '0');
    f(Kind.Int8, 0, '0');
    f(Kind.Int16, 0, '0');
    f(Kind.Int32, 0, '0');
    f(Kind.Int64, 0, '0');
    f(Kind.Float32, 0, '0');
    f(Kind.Float64, 0, '0');

    f(Kind.Int64, 1e18, '1000000000000000000');
    f(Kind.Uint64, 1e19, '10000000000000000000');
    f(Kind.Float64, 1e19, '10000000000000000000');
    f(Kind.Float64, 1e20, '1e+20');

    f(Kind.String, 'hi', 'hi');
  });

  test('write simple blob', () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);
    w.writeTopLevel(makePrimitiveType(Kind.Blob), new Uint8Array([0x00, 0x01]).buffer);
    assert.deepEqual([Kind.Blob, false, 'AAE='], w.array);
  });

  test('write list', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    let l = new ListLeaf(ms, tr, [0, 1, 2, 3]);
    w.writeTopLevel(tr, l);
    assert.deepEqual([Kind.List, Kind.Int32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write list of list', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let it = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int16));
    let tr = makeCompoundType(Kind.List, it);
    let v = new ListLeaf(ms, tr, [new ListLeaf(ms, it, [0]), new ListLeaf(ms, it, [1, 2, 3])]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Int16, false, [false, ['0'], false, ['1', '2', '3']]], w.array);
  });

  test('write set', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Uint32));
    let v = new SetLeaf(ms, tr, [0, 1, 2, 3]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Uint32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write set of set', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let st = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Int32));
    let tr = makeCompoundType(Kind.Set, st);
    let v = new SetLeaf(ms, tr, [new SetLeaf(ms, st, [0]), new SetLeaf(ms, st, [1, 2, 3])]);

    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Int32, false, [false, ['0'], false, ['1', '2', '3']]], w.array);
  });

  test('write map', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Bool));
    let v = new MapLeaf(ms, tr, [{key: 'a', value: false}, {key:'b', value:true}]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, false, ['a', false, 'b', true]], w.array);
  });

  test('write map of map', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let kt = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Int64));
    let vt = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Bool));
    let tr = makeCompoundType(Kind.Map, kt, vt);


    let s = new SetLeaf(ms, vt, [true]);
    let m1 = new MapLeaf(ms, kt, [{key: 'a', value: 0}]);
    let v = new MapLeaf(ms, tr, [{key: m1, value: s}]);
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.Map, Kind.String, Kind.Int64, Kind.Set, Kind.Bool, false, [false, ['a', '0'], false, [true]]], w.array);
  });

  test('write empty struct', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructType('S', [], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0'], w.array);
  });

  test('write struct', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), false),
      new Field('b', makePrimitiveType(Kind.Bool), false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {x: 42, b: true});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', true], w.array);
  });

  test('write struct optional field', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), true),
      new Field('b', makePrimitiveType(Kind.Bool), false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {x: 42, b: true});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', true, '42', true], w.array);

    v = new Struct(type, typeDef, {b: true});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, true], w.array);
  });

  test('write struct with union', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), false)
    ], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false)
    ]);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {x: 42, s: 'hi'});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '1', 'hi'], w.array);

    v = new Struct(type, typeDef, {x: 42, b: true});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '0', true], w.array);
  });

  test('write struct with list', async() => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    let typeDef = makeStructType('S', [
      new Field('l', ltr, false)
    ], []);
    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {l: new ListLeaf(ms, ltr, ['a', 'b'])});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, ['a', 'b']], w.array);

    v = new Struct(type, typeDef, {l: new ListLeaf(ms, ltr, [])});
    w = new JsonArrayWriter(ms);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, []], w.array);
  });

  test('write struct with struct', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let s2TypeDef = makeStructType('S2', [
      new Field('x', makePrimitiveType(Kind.Int32), false)
    ], []);
    let sTypeDef = makeStructType('S', [
      new Field('s', makeType(new Ref(), 0), false)
    ], []);

    let pkg = new Package([s2TypeDef, sTypeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let s2Type = makeType(pkgRef, 0);
    let sType = makeType(pkgRef, 1);

    let v = new Struct(sType, sTypeDef, {s: new Struct(s2Type, s2TypeDef, {x: 42})});
    w.writeTopLevel(sType, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '1', '42'], w.array);
  });

  test('write enum', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let pkg = new Package([makeEnumType('E', ['a', 'b', 'c'])], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typ = makeType(pkgRef, 0);

    w.writeTopLevel(typ, 1);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '1'], w.array);
  });

  test('write list of enum', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let pkg = new Package([makeEnumType('E', ['a', 'b', 'c'])], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let typ = makeType(pkgRef, 0);
    let listType = makeCompoundType(Kind.List, typ);
    let l = new ListLeaf(ms, listType, [0, 1, 2]);

    w.writeTopLevel(listType, l);
    assert.deepEqual([Kind.List, Kind.Unresolved, pkgRef.toString(), '0', false, ['0', '1', '2']], w.array);
  });

  test('write compound list', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);

    let ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    let r1 = writeValue(new ListLeaf(ms, ltr, [0, 1]), ltr, ms);
    let r2 = writeValue(new ListLeaf(ms, ltr, [2, 3]), ltr, ms);
    let r3 = writeValue(new ListLeaf(ms, ltr, [4, 5]), ltr, ms);
    let tuples = [
      new MetaTuple(r1, 2),
      new MetaTuple(r2, 4),
      new MetaTuple(r3, 6)
    ];
    let l = new CompoundList(ms, ltr, tuples);

    w.writeTopLevel(ltr, l);
    assert.deepEqual([Kind.List, Kind.Int32, true, [r1.toString(), '2', r2.toString(), '4', r3.toString(), '6']], w.array);
  });

  test('write type value', async () => {
    let ms = new MemoryStore();

    let test = (expected: Array<any>, v: Type) => {
      let w = new JsonArrayWriter(ms);
      w.writeTopLevel(v.type, v);
      assert.deepEqual(expected, w.array);
    };

    test([Kind.Type, Kind.Int32], makePrimitiveType(Kind.Int32));
    test([Kind.Type, Kind.List, [Kind.Bool]], makeCompoundType(Kind.List, makePrimitiveType(Kind.Bool)));
    test([Kind.Type, Kind.Map, [Kind.Bool, Kind.String]], makeCompoundType(Kind.Map, makePrimitiveType(Kind.Bool), makePrimitiveType(Kind.String)));
    test([Kind.Type, Kind.Enum, 'E', ['a', 'b', 'c']], makeEnumType('E', ['a', 'b', 'c']));
    test([Kind.Type, Kind.Struct, 'S', ['x', Kind.Int16, false, 'v', Kind.Value, true], []], makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('v', makePrimitiveType(Kind.Value), true)
    ], []));
    test([Kind.Type, Kind.Struct, 'S', [], ['x', Kind.Int16, false, 'v', Kind.Value, false]], makeStructType('S', [], [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('v', makePrimitiveType(Kind.Value), false)
    ]));

    let pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    test([Kind.Type, Kind.Unresolved, pkgRef.toString(), '123'], makeType(pkgRef, 123));

    test([Kind.Type, Kind.Struct, 'S', ['e', Kind.Unresolved, pkgRef.toString(), '123', false, 'x', Kind.Int64, false], []], makeStructType('S', [
      new Field('e', makeType(pkgRef, 123), false),
      new Field('x', makePrimitiveType(Kind.Int64), false)
    ], []));

    // test([Kind.Type, Kind.Unresolved, new Ref().toString(), -1, 'ns', 'n'], makeUnresolvedType('ns', 'n'));
  });

  test('top level blob', () => {
    function stringToBuffer(s) {
      let bytes = new Uint8Array(s.length);
      for (let i = 0; i < s.length; i++) {
        bytes[i] = s.charCodeAt(i);
      }
      return bytes.buffer;
    }

    let ms = new MemoryStore();
    let blob = stringToBuffer('hi');
    let chunk = encodeNomsValue(blob, makePrimitiveType(Kind.Blob), ms);
    assert.equal(4, chunk.data.length);
    assert.deepEqual(stringToBuffer('b hi'), chunk.data.buffer);

    let buffer2 = new ArrayBuffer(2 + 256);
    let view = new DataView(buffer2);
    view.setUint8(0, 'b'.charCodeAt(0));
    view.setUint8(1, ' '.charCodeAt(0));
    let bytes = new Uint8Array(256);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
      view.setUint8(2 + i, i);
    }
    let blob2 = bytes.buffer;
    let chunk2 = encodeNomsValue(blob2, makePrimitiveType(Kind.Blob), ms);
    assert.equal(buffer2.byteLength, chunk2.data.buffer.byteLength);
    assert.deepEqual(buffer2, chunk2.data.buffer);
  });

  test('write ref', async () => {
    let ms = new MemoryStore();
    let w = new JsonArrayWriter(ms);
    let ref = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    let t = makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Blob));
    w.writeTopLevel(t, ref);
    assert.deepEqual([Kind.Ref, Kind.Blob, ref.toString()], w.array);
  });
});
