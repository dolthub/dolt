// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import RefValue from './ref-value.js';
import {newStruct} from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {encodeNomsValue, JsonArrayWriter} from './encode.js';
import {
  blobType,
  boolType,
  Field,
  float32Type,
  float64Type,
  int16Type,
  int32Type,
  int64Type,
  int8Type,
  makeCompoundType,
  makeEnumType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeType,
  stringType,
  Type,
  uint16Type,
  uint32Type,
  uint64Type,
  uint8Type,
  valueType,
} from './type.js';
import {IndexedMetaSequence, MetaTuple} from './meta-sequence.js';
import {Kind} from './noms-kind.js';
import {newList, ListLeafSequence, NomsList} from './list.js';
import {newMap, MapLeafSequence, NomsMap} from './map.js';
import {newSet, NomsSet, SetLeafSequence} from './set.js';
import {Package, registerPackage} from './package.js';
import {newBlob} from './blob.js';
import DataStore from './data-store.js';

suite('Encode', () => {
  test('write primitives', () => {
    function f(k: NomsKind, t:Type, v: any, ex: any) {
      const ms = new MemoryStore();
      const ds = new DataStore(ms);
      const w = new JsonArrayWriter(ds);
      w.writeTopLevel(t, v);
      assert.deepEqual([k, ex], w.array);
    }

    f(Kind.Bool, boolType, true, true);
    f(Kind.Bool, boolType, false, false);

    f(Kind.Uint8, uint8Type, 0, '0');
    f(Kind.Uint16, uint16Type, 0, '0');
    f(Kind.Uint32, uint32Type, 0, '0');
    f(Kind.Uint64, uint64Type, 0, '0');
    f(Kind.Int8, int8Type, 0, '0');
    f(Kind.Int16, int16Type, 0, '0');
    f(Kind.Int32, int32Type, 0, '0');
    f(Kind.Int64, int64Type, 0, '0');
    f(Kind.Float32, float32Type, 0, '0');
    f(Kind.Float64, float64Type, 0, '0');

    f(Kind.Int64, int64Type, 1e18, '1000000000000000000');
    f(Kind.Uint64, uint64Type, 1e19, '10000000000000000000');
    f(Kind.Float64, float64Type, 1e19, '10000000000000000000');
    f(Kind.Float64, float64Type, 1e20, '1e+20');

    f(Kind.String, stringType, 'hi', 'hi');
  });

  test('write simple blob', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const blob = await newBlob(new Uint8Array([0x00, 0x01]));
    w.writeTopLevel(blobType, blob);
    assert.deepEqual([Kind.Blob, false, 'AAE='], w.array);
  });

  test('write list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.List, int32Type);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, l);
    assert.deepEqual([Kind.List, Kind.Int32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write list of value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.List, valueType);
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

    const it = makeCompoundType(Kind.List, int16Type);
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

    const tr = makeCompoundType(Kind.Set, uint32Type);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Uint32, false, ['0', '1', '2', '3']], w.array);
  });

  test('write set of set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const st = makeCompoundType(Kind.Set, int32Type);
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

    const tr = makeCompoundType(Kind.Map, stringType, boolType);
    const v = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'a', value: false},
        {key:'b', value:true}]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, false, ['a', false, 'b', true]], w.array);
  });

  test('write map of map', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const kt = makeCompoundType(Kind.Map, stringType, int64Type);
    const vt = makeCompoundType(Kind.Set, boolType);
    const tr = makeCompoundType(Kind.Map, kt, vt);

    const s = new NomsSet(vt, new SetLeafSequence(ds, vt, [true]));
    const m1 = new NomsMap(kt, new MapLeafSequence(ds, kt, [{key: 'a', value: 0}]));
    const v = new NomsMap(kt, new MapLeafSequence(ds, tr, [{key: m1, value: s}]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.Map, Kind.String, Kind.Int64, Kind.Set, Kind.Bool, false,
        [false, ['a', '0'], false, [true]]], w.array);
  });

  test('write empty struct', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const v = newStruct(type, typeDef, {});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0'], w.array);
  });

  test('write struct', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', int8Type, false),
      new Field('b', boolType, false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const v = newStruct(type, typeDef, {x: 42, b: true});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', true], w.array);
  });

  test('write struct optional field', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', int8Type, true),
      new Field('b', boolType, false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = newStruct(type, typeDef, {x: 42, b: true});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', true, '42', true], w.array);

    v = newStruct(type, typeDef, {b: true});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, true], w.array);
  });

  test('write struct with union', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const typeDef = makeStructType('S', [
      new Field('x', int8Type, false),
    ], [
      new Field('b', boolType, false),
      new Field('s', stringType, false),
    ]);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = newStruct(type, typeDef, {x: 42, s: 'hi'});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '1', 'hi'], w.array);

    v = newStruct(type, typeDef, {x: 42, b: true});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', '42', '0', true], w.array);
  });

  test('write struct with list', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const ltr = makeCompoundType(Kind.List, stringType);
    const typeDef = makeStructType('S', [
      new Field('l', ltr, false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    let v = newStruct(type, typeDef, {l: new NomsList(ltr,
          new ListLeafSequence(ds, ltr, ['a', 'b']))});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, ['a', 'b']], w.array);

    v = newStruct(type, typeDef, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, []))});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Unresolved, pkgRef.toString(), '0', false, []], w.array);
  });

  test('write struct with struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const s2TypeDef = makeStructType('S2', [
      new Field('x', int32Type, false),
    ], []);
    const sTypeDef = makeStructType('S', [
      new Field('s', makeType(new Ref(), 0), false),
    ], []);

    const pkg = new Package([s2TypeDef, sTypeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const s2Type = makeType(pkgRef, 0);
    const sType = makeType(pkgRef, 1);

    const v = newStruct(sType, sTypeDef, {s: newStruct(s2Type, s2TypeDef, {x: 42})});
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

    const ltr = makeCompoundType(Kind.List, int32Type);
    const r1 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0, 1])));
    const r2 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [2, 3])));
    const r3 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [4, 5])));
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

    test([Kind.Type, Kind.Int32], int32Type);
    test([Kind.Type, Kind.List, [Kind.Bool]],
         makeCompoundType(Kind.List, boolType));
    test([Kind.Type, Kind.Map, [Kind.Bool, Kind.String]],
         makeCompoundType(Kind.Map, boolType, stringType));
    test([Kind.Type, Kind.Enum, 'E', ['a', 'b', 'c']], makeEnumType('E', ['a', 'b', 'c']));
    test([Kind.Type, Kind.Struct, 'S', ['x', Kind.Int16, false, 'v', Kind.Value, true], []],
         makeStructType('S', [
           new Field('x', int16Type, false),
           new Field('v', valueType, true),
         ], []));
    test([Kind.Type, Kind.Struct, 'S', [], ['x', Kind.Int16, false, 'v', Kind.Value, false]],
         makeStructType('S', [], [
           new Field('x', int16Type, false),
           new Field('v', valueType, false),
         ]));

    const pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    test([Kind.Type, Kind.Unresolved, pkgRef.toString(), '123'], makeType(pkgRef, 123));

    test([Kind.Type, Kind.Struct, 'S',
          ['e', Kind.Unresolved, pkgRef.toString(), '123', false, 'x', Kind.Int64, false], []],
          makeStructType('S', [
            new Field('e', makeType(pkgRef, 123), false),
            new Field('x', int64Type, false),
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

    const chunk = encodeNomsValue(blob, blobType, ds);
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
    const chunk2 = encodeNomsValue(blob2, blobType, ds);
    assert.equal(buffer2.byteLength, chunk2.data.buffer.byteLength);
    assert.deepEqual(buffer2, chunk2.data.buffer);
  });

  test('write ref', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const ref = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const t = makeCompoundType(Kind.Ref, blobType);
    const v = new RefValue(ref, t);
    w.writeTopLevel(t, v);

    assert.deepEqual([Kind.Ref, Kind.Blob, ref.toString()], w.array);
  });

  test('type errors', async () => {
    const ds = new DataStore(new MemoryStore());
    const w = new JsonArrayWriter(ds);

    const test = (et, at, t, v) => {
      try {
        w.writeTopLevel(t, v);
      } catch (ex) {
        assert.equal(ex.message, `Failed to write ${et}. Invalid type: ${at}`);
        return;
      }
      assert.ok(false, `Expected error, 'Failed to write ${et}. Invalid type: ${at}' but Got none`);
    };

    test('Int8', 'string', int8Type, 'hi');
    test('Float64', 'string', float64Type, 'hi');
    test('Bool', 'string', boolType, 'hi');
    test('Blob', 'string', blobType, 'hi');

    test('String', 'number', stringType, 42);
    test('Bool', 'number', boolType, 42);
    test('Blob', 'number', blobType, 42);

    test('Int8', 'boolean', int8Type, true);
    test('Float64', 'boolean', float64Type, true);
    test('String', 'boolean', stringType, true);
    test('Blob', 'boolean', blobType, true);

    const blob = await newBlob(new Uint8Array([0, 1]));
    test('Int8', 'Blob', int8Type, blob);
    test('Float64', 'Blob', float64Type, blob);
    test('String', 'Blob', stringType, blob);
    test('Bool', 'Blob', boolType, blob);

    const list = await newList([0, 1], makeListType(int8Type));
    test('Int8', 'List<Int8>', int8Type, list);
    test('Float64', 'List<Int8>', float64Type, list);
    test('String', 'List<Int8>', stringType, list);
    test('Bool', 'List<Int8>', boolType, list);
    test('Blob', 'List<Int8>', blobType, list);

    const map = await newMap(['zero', 1], makeMapType(stringType, int8Type));
    test('Int8', 'Map<String, Int8>', int8Type, map);
    test('Float64', 'Map<String, Int8>', float64Type, map);
    test('String', 'Map<String, Int8>', stringType, map);
    test('Bool', 'Map<String, Int8>', boolType, map);
    test('Blob', 'Map<String, Int8>', blobType, map);

    const set = await newSet([0, 1], makeSetType(int8Type));
    test('Int8', 'Set<Int8>', int8Type, set);
    test('Float64', 'Set<Int8>', float64Type, set);
    test('String', 'Set<Int8>', stringType, set);
    test('Bool', 'Set<Int8>', boolType, set);
    test('Blob', 'Set<Int8>', blobType, set);
  });
});
