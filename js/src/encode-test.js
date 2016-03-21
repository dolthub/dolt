// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import test from './async-test.js';
import type {NomsKind} from './noms-kind.js';
import {encodeNomsValue, JsonArrayWriter} from './encode.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, Type,}
    from './type.js';
import {IndexedMetaSequence, MetaTuple} from './meta-sequence.js';
import {Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {Package, registerPackage} from './package.js';
import {writeValue} from './encode.js';
import {newBlob} from './blob.js';
import {DataStore} from './data-store.js';

suite('Encode', () => {
  test('write primitives', () => {
    function f(k: NomsKind, v: any, ex: any) {
      const ms = new MemoryStore();
      const ds = new DataStore(ms);
      const w = new JsonArrayWriter(ds);
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

  test('write simple blob', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const blob = await newBlob(new Uint8Array([0x00, 0x01]));
    w.writeTopLevel(makePrimitiveType(Kind.Blob), blob);
    assert.deepEqual([Kind.Blob, false, 'AAE='], w.array);
  });

  test('write list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, l);
    assert.deepEqual([Kind.List, Kind.Int32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write list of value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Value));
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, ['0', '1', '2', '3']));
    w.writeTopLevel(tr, l);
    assert.deepEqual([Kind.List, Kind.Value, false, [
      Kind.String, '0',
      Kind.String, '1',
      Kind.String, '2',
      Kind.String, '3',
    ]], w.array);
  });

  test('write list of list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const it = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int16));
    const tr = makeCompoundType(Kind.List, it);
    const v = new NomsList(tr, new ListLeafSequence(ds, tr, [
      new NomsList(tr, new ListLeafSequence(ds, it, [0])),
      new NomsList(tr, new ListLeafSequence(ds, it, [1, 2, 3])),
    ]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Int16, false, [false, ['0'], false,
        ['1', '2', '3']]], w.array);
  });

  test('write set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Uint32));
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Uint32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write set of set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const st = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Int32));
    const tr = makeCompoundType(Kind.Set, st);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [
      new NomsSet(tr, new SetLeafSequence(ds, st, [0])),
      new NomsSet(tr, new SetLeafSequence(ds, st, [1, 2, 3])),
    ]));

    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Int32, false, [false, ['0'], false,
        ['1', '2', '3']]], w.array);
  });

  test('write map', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
        makePrimitiveType(Kind.Bool));
    const v = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'a', value: false},
        {key:'b', value:true}]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, false, ['a', false, 'b', true]], w.array);
  });

  test('write map of map', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const kt = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
        makePrimitiveType(Kind.Int64));
    const vt = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Bool));
    const tr = makeCompoundType(Kind.Map, kt, vt);

    const s = new NomsSet(vt, new SetLeafSequence(ds, vt, [true]));
    const m1 = new NomsMap(kt, new MapLeafSequence(ds, kt, [{key: 'a', value: 0}]));
    const v = new NomsMap(kt, new MapLeafSequence(ds, tr, [{key: m1, value: s}]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.Map, Kind.String, Kind.Int64, Kind.Set, Kind.Bool, false,
        [false, ['a', '0'], false, [true]]], w.array);
  });

  test('write empty struct', async() => {
    const ms = new MemoryStore();const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const v = new Struct(type, typeDef, {});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0'], w.array);
  });

  test('write struct', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), false),
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const v = new Struct(type, typeDef, {x: 42, b: true});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', true], w.array);
  });

  test('write struct optional field', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), true),
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {x: 42, b: true});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', true, '42', true], w.array);

    v = new Struct(type, typeDef, {b: true});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, true], w.array);
  });

  test('write struct with union', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', makePrimitiveType(Kind.Int8), false),
    ], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ]);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {x: 42, s: 'hi'});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '1', 'hi'], w.array);

    v = new Struct(type, typeDef, {x: 42, b: true});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '0', true], w.array);
  });

  test('write struct with list', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const typeDef = makeStructType('S', [
      new Field('l', ltr, false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = new Struct(type, typeDef, {l: new NomsList(ltr,
          new ListLeafSequence(ds, ltr, ['a', 'b']))});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, ['a', 'b']], w.array);

    v = new Struct(type, typeDef, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, []))});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, []], w.array);
  });

  test('write struct with struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const s2TypeDef = makeStructType('S2', [
      new Field('x', makePrimitiveType(Kind.Int32), false),
    ], []);
    const sTypeDef = makeStructType('S', [
      new Field('s', makeType(new Ref(), 0), false),
    ], []);

    const pkg = new Package([s2TypeDef, sTypeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const s2Type = makeType(pkgRef, 0);
    const sType = makeType(pkgRef, 1);

    const v = new Struct(sType, sTypeDef, {s: new Struct(s2Type, s2TypeDef, {x: 42})});
    w.writeTopLevel(sType, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '1', '42'], w.array);
  });

  test('write enum', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const pkg = new Package([makeEnumType('E', ['a', 'b', 'c'])], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const typ = makeType(pkgRef, 0);

    w.writeTopLevel(typ, 1);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '1'], w.array);
  });

  test('write list of enum', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const pkg = new Package([makeEnumType('E', ['a', 'b', 'c'])], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const typ = makeType(pkgRef, 0);
    const listType = makeCompoundType(Kind.List, typ);
    const l = new NomsList(listType, new ListLeafSequence(ds, listType, [0, 1, 2]));

    w.writeTopLevel(listType, l);
    assert.deepEqual([Kind.List, Kind.Unresolved, pkgRef.toString(), '0', false, ['0', '1', '2']],
        w.array);
  });

  test('write compound list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const r1 = writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0, 1])), ltr, ds);
    const r2 = writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [2, 3])), ltr, ds);
    const r3 = writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [4, 5])), ltr, ds);
    const tuples = [
      new MetaTuple(r1, 2),
      new MetaTuple(r2, 4),
      new MetaTuple(r3, 6),
    ];
    const l = new NomsList(ltr, new IndexedMetaSequence(ds, ltr, tuples));

    w.writeTopLevel(ltr, l);
    assert.deepEqual([Kind.List, Kind.Int32, true, [r1.toString(), '2', r2.toString(), '4',
        r3.toString(), '6']], w.array);
  });

  test('write type value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const test = (expected: Array<any>, v: Type) => {
      const w = new JsonArrayWriter(ds);
      w.writeTopLevel(v.type, v);
      assert.deepEqual(expected, w.array);
    };

    test([Kind.Type, Kind.Int32], makePrimitiveType(Kind.Int32));
    test([Kind.Type, Kind.List, [Kind.Bool]],
         makeCompoundType(Kind.List, makePrimitiveType(Kind.Bool)));
    test([Kind.Type, Kind.Map, [Kind.Bool, Kind.String]],
         makeCompoundType(Kind.Map, makePrimitiveType(Kind.Bool), makePrimitiveType(Kind.String)));
    test([Kind.Type, Kind.Enum, 'E', ['a', 'b', 'c']], makeEnumType('E', ['a', 'b', 'c']));
    test([Kind.Type, Kind.Struct, 'S', ['x', Kind.Int16, false, 'v', Kind.Value, true], []],
         makeStructType('S', [
           new Field('x', makePrimitiveType(Kind.Int16), false),
           new Field('v', makePrimitiveType(Kind.Value), true),
         ], []));
    test([Kind.Type, Kind.Struct, 'S', [], ['x', Kind.Int16, false, 'v', Kind.Value, false]],
         makeStructType('S', [], [
           new Field('x', makePrimitiveType(Kind.Int16), false),
           new Field('v', makePrimitiveType(Kind.Value), false),
         ]));

    const pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    test([Kind.Type, Kind.Unresolved, pkgRef.toString(), '123'], makeType(pkgRef, 123));

    test([Kind.Type, Kind.Struct, 'S',
          ['e', Kind.Unresolved, pkgRef.toString(), '123', false, 'x', Kind.Int64, false], []],
          makeStructType('S', [
            new Field('e', makeType(pkgRef, 123), false),
            new Field('x', makePrimitiveType(Kind.Int64), false),
          ], []));

    // test([Kind.Type, Kind.Unresolved, new Ref().toString(), -1, 'ns', 'n'],
    //      makeUnresolvedType('ns', 'n'));
  });

  test('top level blob', async () => {
    function stringToUint8Array(s) {
      const bytes = new Uint8Array(s.length);
      for (let i = 0; i < s.length; i++) {
        bytes[i] = s.charCodeAt(i);
      }
      return bytes;
    }

    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const blob = await newBlob(stringToUint8Array('hi'));

    const chunk = encodeNomsValue(blob, makePrimitiveType(Kind.Blob), ds);
    assert.equal(4, chunk.data.length);
    assert.deepEqual(stringToUint8Array('b hi'), chunk.data);

    const buffer2 = new ArrayBuffer(2 + 256);
    const view = new DataView(buffer2);
    view.setUint8(0, 'b'.charCodeAt(0));
    view.setUint8(1, ' '.charCodeAt(0));
    const bytes = new Uint8Array(256);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
      view.setUint8(2 + i, i);
    }
    const blob2 = await newBlob(bytes);
    const chunk2 = encodeNomsValue(blob2, makePrimitiveType(Kind.Blob), ds);
    assert.equal(buffer2.byteLength, chunk2.data.buffer.byteLength);
    assert.deepEqual(buffer2, chunk2.data.buffer);
  });

  test('write ref', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const ref = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const t = makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Blob));
    w.writeTopLevel(t, ref);
    assert.deepEqual([Kind.Ref, Kind.Blob, ref.toString()], w.array);
  });
});
