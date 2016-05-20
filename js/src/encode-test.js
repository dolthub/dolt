// @flow

import {assert} from 'chai';
import {suite, setup, teardown, test} from 'mocha';

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import Ref from './ref.js';
import {constructRefValue} from './ref-value.js';
import {newStruct} from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {encodeNomsValue, JsonArrayWriter} from './encode.js';
import {
  blobType,
  boolType,
  makeListType,
  makeMapType,
  makeRefType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  Type,
  valueType,
} from './type.js';
import {newListMetaSequence, MetaTuple, newSetMetaSequence} from './meta-sequence.js';
import {Kind} from './noms-kind.js';
import List, {newListLeafSequence} from './list.js';
import Map, {newMapLeafSequence} from './map.js';
import Set, {newSetLeafSequence} from './set.js';
import {newBlob} from './blob.js';
import Database from './database.js';
import type {valueOrPrimitive} from './value.js';

suite('Encode', () => {
  let db;

  setup(() => {
    db = new Database(makeTestingBatchStore());
  });

  teardown((): Promise<void> => db.close());

  test('write primitives', () => {
    function f(k: NomsKind, v: valueOrPrimitive, ex: valueOrPrimitive) {
      const w = new JsonArrayWriter(db);
      w.writeValue(v);
      assert.deepEqual([k, ex], w.array);
    }

    f(Kind.Bool, true, true);
    f(Kind.Bool, false, false);

    f(Kind.Number, 0, '0');

    f(Kind.Number, 1e18, '1000000000000000000');
    f(Kind.Number, 1e19, '10000000000000000000');
    f(Kind.Number, 1e20, '1e+20');

    f(Kind.String, 'hi', 'hi');
  });

  test('write simple blob', async () => {
    const w = new JsonArrayWriter(db);
    const blob = await newBlob(new Uint8Array([0x00, 0x01]));
    w.writeValue(blob);
    assert.deepEqual([Kind.Blob, false, 'AAE='], w.array);
  });

  test('write list', () => {
    const w = new JsonArrayWriter(db);

    const l = new List(newListLeafSequence(db, [0, 1, 2, 3]));
    w.writeValue(l);
    assert.deepEqual([Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']], w.array);
  });

  test('write list of value', () => {
    const w = new JsonArrayWriter(db);

    const l = new List(newListLeafSequence(db, ['0', 1, '2', true]));
    w.writeValue(l);
    assert.deepEqual([Kind.List, Kind.Union, 3, Kind.Bool, Kind.Number, Kind.String, false, [
      Kind.String, '0',
      Kind.Number, '1',
      Kind.String, '2',
      Kind.Bool, true,
    ]], w.array);
  });

  test('write list of list', () => {
    const w = new JsonArrayWriter(db);

    const v = new List(newListLeafSequence(db, [
      new List(newListLeafSequence(db, [0])),
      new List(newListLeafSequence(db, [1, 2, 3])),
    ]));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Number, false, [
      Kind.List, Kind.Number, false, [Kind.Number, '0'],
      Kind.List, Kind.Number, false, [Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']]],
      w.array);
  });

  test('write leaf set', () => {
    const w = new JsonArrayWriter(db);

    const v = new Set(newSetLeafSequence(db, [0, 1, 2, 3]));
    w.writeValue(v);
    assert.deepEqual([Kind.Set, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']], w.array);
  });

  test('write compound set', () => {
    const w = new JsonArrayWriter(db);
    const r1 = db.writeValue(new Set(newSetLeafSequence(db, [0])));
    const r2 = db.writeValue(new Set(newSetLeafSequence(db, [1, 2])));
    const r3 = db.writeValue(new Set(newSetLeafSequence(db, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 0, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 5, 3),
    ];
    const l = new Set(newSetMetaSequence(db, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.Set, Kind.Number, true, [
        Kind.Ref, Kind.Set, Kind.Number, r1.targetRef.toString(), '1', Kind.Number, '0', '1',
        Kind.Ref, Kind.Set, Kind.Number, r2.targetRef.toString(), '1', Kind.Number, '2', '2',
        Kind.Ref, Kind.Set, Kind.Number, r3.targetRef.toString(), '1', Kind.Number, '5', '3',
      ],
    ], w.array);
  });

  test('write set of set', () => {
    const w = new JsonArrayWriter(db);

    const v = new Set(newSetLeafSequence(db, [
      new Set(newSetLeafSequence(db, [0])),
      new Set(newSetLeafSequence(db, [1, 2, 3])),
    ]));

    w.writeValue(v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Number, false, [
      Kind.Set, Kind.Number, false, [Kind.Number, '0'],
      Kind.Set, Kind.Number, false, [Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']]],
      w.array);
  });

  test('write map', () => {
    const w = new JsonArrayWriter(db);

    const v = new Map(newMapLeafSequence(db, [['a', false], ['b', true]]));
    w.writeValue(v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, false,
      [Kind.String, 'a', Kind.Bool, false, Kind.String, 'b', Kind.Bool, true]], w.array);
  });

  test('write map of map', () => {
    const w = new JsonArrayWriter(db);

    // Map<Map<String, Number>, Set<Bool>>({{'a': 0}: {true}})
    const s = new Set(newSetLeafSequence(db, [true]));
    const m1 = new Map(newMapLeafSequence(db, [['a', 0]]));
    const v = new Map(newMapLeafSequence(db, [[m1, s]]));
    w.writeValue(v);
    assert.deepEqual([Kind.Map,
      Kind.Map, Kind.String, Kind.Number,
      Kind.Set, Kind.Bool, false, [
        Kind.Map, Kind.String, Kind.Number, false, [Kind.String, 'a', Kind.Number, '0'],
        Kind.Set, Kind.Bool, false, [Kind.Bool, true]]], w.array);
  });

  test('write empty struct', () => {
    const w = new JsonArrayWriter(db);

    const v = newStruct('S', {});

    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', []], w.array);
  });

  test('write struct', () => {
    const w = new JsonArrayWriter(db);
    const v = newStruct('S', {x: 42, b: true});

    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['b', Kind.Bool, 'x', Kind.Number],
      Kind.Bool, true, Kind.Number, '42'], w.array);
  });

  test('write struct with list', () => {
    let w = new JsonArrayWriter(db);

    let v = newStruct('S', {l: new List(newListLeafSequence(db, ['a', 'b']))});
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.String],
      Kind.List, Kind.String, false, [Kind.String, 'a', Kind.String, 'b']], w.array);

    v = newStruct('S', {l: new List(newListLeafSequence(db, []))});
    w = new JsonArrayWriter(db);
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.Union, 0],
      Kind.List, Kind.Union, 0, false, []], w.array);
  });

  test('write struct with struct', () => {
    const w = new JsonArrayWriter(db);

    const v = newStruct('S', {s: newStruct('S2', {x: 42})});
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S',
      ['s', Kind.Struct, 'S2', ['x', Kind.Number]],
      Kind.Struct, 'S2', ['x', Kind.Number], Kind.Number, '42'], w.array);
  });

  test('write compound list', () => {
    const w = new JsonArrayWriter(db);
    const r1 = db.writeValue(new List(newListLeafSequence(db, [0])));
    const r2 = db.writeValue(new List(newListLeafSequence(db, [1, 2])));
    const r3 = db.writeValue(new List(newListLeafSequence(db, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l = new List(newListMetaSequence(db, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.List, Kind.Number, true, [
        Kind.Ref, Kind.List, Kind.Number, r1.targetRef.toString(), '1', Kind.Number, '1', '1',
        Kind.Ref, Kind.List, Kind.Number, r2.targetRef.toString(), '1', Kind.Number, '2', '2',
        Kind.Ref, Kind.List, Kind.Number, r3.targetRef.toString(), '1', Kind.Number, '3', '3',
      ],
    ], w.array);
  });

  test('write compound set with bool', () => {
    const w = new JsonArrayWriter(db);
    const r1 = db.writeValue(new Set(newSetLeafSequence(db, [true])));
    const r2 = db.writeValue(new Set(newSetLeafSequence(db, [false])));
    const tuples = [
      new MetaTuple(r1, true, 1),
      new MetaTuple(r2, false, 1),
    ];
    const l = new Set(newSetMetaSequence(db, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.Set, Kind.Bool, true, [
        Kind.Ref, Kind.Set, Kind.Bool, r1.targetRef.toString(), '1', Kind.Bool, true, '1',
        Kind.Ref, Kind.Set, Kind.Bool, r2.targetRef.toString(), '1', Kind.Bool, false, '1',
      ],
    ], w.array);
  });

  test('write type value', () => {

    const test = (expected: Array<any>, v: Type) => {
      const w = new JsonArrayWriter(db);
      w.writeValue(v);
      assert.deepEqual(w.array, expected);
    };

    test([Kind.Type, Kind.Number], numberType);
    test([Kind.Type, Kind.List, Kind.Bool],
         makeListType(boolType));
    test([Kind.Type, Kind.Map, Kind.Bool, Kind.String],
         makeMapType(boolType, stringType));
    test([Kind.Type, Kind.Struct, 'S', ['v', Kind.Value, 'x', Kind.Number]],
         makeStructType('S', {
           'x': numberType,
           'v': valueType,
         }));

    // struct A6 {
    //   v: Number
    //   cs: List<A6>
    // }

    const st = makeStructType('A6', {
      'v': numberType,
      'cs': valueType, // placeholder
    });
    const lt = makeListType(st);
    st.desc.fields['cs'] = lt;

    test([Kind.Type, Kind.Struct, 'A6', ['cs', Kind.List, Kind.Parent, 0, 'v', Kind.Number]], st);

    test([Kind.Type, Kind.Union, 0], makeUnionType([]));
    test([Kind.Type, Kind.Union, 2, Kind.Number, Kind.String],
         makeUnionType([numberType, stringType]));
    test([Kind.Type, Kind.List, Kind.Union, 0], makeListType(makeUnionType([])));
  });

  test('top level blob', async () => {
    function stringToUint8Array(s) {
      const bytes = new Uint8Array(s.length);
      for (let i = 0; i < s.length; i++) {
        bytes[i] = s.charCodeAt(i);
      }
      return bytes;
    }

    const blob = await newBlob(stringToUint8Array('hi'));

    const chunk = encodeNomsValue(blob, db);
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
    const chunk2 = encodeNomsValue(blob2, db);
    assert.equal(buffer2.byteLength, chunk2.data.buffer.byteLength);
    assert.deepEqual(buffer2, chunk2.data.buffer);
  });

  test('write ref', () => {
    const w = new JsonArrayWriter(db);
    const ref = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const t = makeRefType(blobType);
    const v = constructRefValue(t, ref, 1);
    w.writeValue(v);

    assert.deepEqual([Kind.Ref, Kind.Blob, ref.toString(), '1'], w.array);
  });

  test('write union list', () => {
    const w = new JsonArrayWriter(db);
    const v = new List(newListLeafSequence(db, ['hi', 42]));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.Union, 2, Kind.Number, Kind.String,
      false, [Kind.String, 'hi', Kind.Number, '42']], w.array);
  });

  test('write empty union list', () => {
    const w = new JsonArrayWriter(db);
    const v = new List(newListLeafSequence(db, []));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.Union, 0, false, []], w.array);
  });
});
