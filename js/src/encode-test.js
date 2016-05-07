// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

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
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  Type,
  valueType,
} from './type.js';
import {IndexedMetaSequence, MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import {Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {newBlob} from './blob.js';
import Database from './database.js';
import type {valueOrPrimitive} from './value.js';

suite('Encode', () => {
  test('write primitives', () => {
    function f(k: NomsKind, v: valueOrPrimitive, ex: valueOrPrimitive) {
      const ds = new Database(makeTestingBatchStore());
      const w = new JsonArrayWriter(ds);
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
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const blob = await newBlob(new Uint8Array([0x00, 0x01]));
    w.writeValue(blob);
    assert.deepEqual([Kind.Blob, false, 'AAE='], w.array);
  });

  test('write list', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const tr = makeListType(numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeValue(l);
    assert.deepEqual([Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']], w.array);
  });

  test('write list of value', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const tr = makeListType(valueType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, ['0', 1, '2', true]));
    w.writeValue(l);
    assert.deepEqual([Kind.List, Kind.Value, false, [
      Kind.String, '0',
      Kind.Number, '1',
      Kind.String, '2',
      Kind.Bool, true,
    ]], w.array);
  });

  test('write list of list', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const it = makeListType(numberType);
    const tr = makeListType(it);
    const v = new NomsList(tr, new ListLeafSequence(ds, tr, [
      new NomsList(it, new ListLeafSequence(ds, it, [0])),
      new NomsList(it, new ListLeafSequence(ds, it, [1, 2, 3])),
    ]));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Number, false, [
      Kind.List, Kind.Number, false, [Kind.Number, '0'],
      Kind.List, Kind.Number, false, [Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']]],
      w.array);
  });

  test('write leaf set', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const tr = makeSetType(numberType);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeValue(v);
    assert.deepEqual([Kind.Set, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']], w.array);
  });

  test('write compound set', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const ltr = makeSetType(numberType);
    const r1 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [0])));
    const r2 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [1, 2])));
    const r3 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 0, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 5, 3),
    ];
    const l = new NomsSet(ltr, new OrderedMetaSequence(ds, ltr, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.Set, Kind.Number, true, [
        Kind.Ref, Kind.Set, Kind.Number, r1.targetRef.toString(), '1', Kind.Number, '0', '1',
        Kind.Ref, Kind.Set, Kind.Number, r2.targetRef.toString(), '1', Kind.Number, '2', '2',
        Kind.Ref, Kind.Set, Kind.Number, r3.targetRef.toString(), '1', Kind.Number, '5', '3',
      ],
    ], w.array);
  });

  test('write set of set', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const st = makeSetType(numberType);
    const tr = makeSetType(st);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [
      new NomsSet(st, new SetLeafSequence(ds, st, [0])),
      new NomsSet(st, new SetLeafSequence(ds, st, [1, 2, 3])),
    ]));

    w.writeValue(v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Number, false, [
      Kind.Set, Kind.Number, false, [Kind.Number, '0'],
      Kind.Set, Kind.Number, false, [Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']]],
      w.array);
  });

  test('write map', async() => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const tr = makeMapType(stringType, boolType);
    const v = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'a', value: false},
        {key:'b', value:true}]));
    w.writeValue(v);
    assert.deepEqual([Kind.Map, Kind.String, Kind.Bool, false,
      [Kind.String, 'a', Kind.Bool, false, Kind.String, 'b', Kind.Bool, true]], w.array);
  });

  test('write map of map', async() => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const kt = makeMapType(stringType, numberType);
    const vt = makeSetType(boolType);
    const tr = makeMapType(kt, vt);

    // Map<Map<String, Number>, Set<Bool>>({{'a': 0}: {true}})
    const s = new NomsSet(vt, new SetLeafSequence(ds, vt, [true]));
    const m1 = new NomsMap(kt, new MapLeafSequence(ds, kt, [{key: 'a', value: 0}]));
    const v = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: m1, value: s}]));
    w.writeValue(v);
    assert.deepEqual([Kind.Map,
      Kind.Map, Kind.String, Kind.Number,
      Kind.Set, Kind.Bool, false, [
        Kind.Map, Kind.String, Kind.Number, false, [Kind.String, 'a', Kind.Number, '0'],
        Kind.Set, Kind.Bool, false, [Kind.Bool, true]]], w.array);
  });

  test('write empty struct', async() => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const type = makeStructType('S', {});
    const v = newStruct(type, {});

    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', []], w.array);
  });

  test('write struct', async() => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const type = makeStructType('S', {
      'x': numberType,
      'b': boolType,
    });

    const v = newStruct(type, {x: 42, b: true});

    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['b', Kind.Bool, 'x', Kind.Number],
      Kind.Bool, true, Kind.Number, '42'], w.array);
  });

  test('write struct with list', async() => {
    const ds = new Database(makeTestingBatchStore());
    let w = new JsonArrayWriter(ds);

    const ltr = makeListType(stringType);
    const type = makeStructType('S', {
      'l': ltr,
    });

    let v = newStruct(type, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, ['a', 'b']))});
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.String],
      Kind.List, Kind.String, false, [Kind.String, 'a', Kind.String, 'b']], w.array);

    v = newStruct(type, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, []))});
    w = new JsonArrayWriter(ds);
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.String],
      Kind.List, Kind.String, false, []], w.array);
  });

  test('write struct with struct', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);

    const s2Type = makeStructType('S2', {
      'x': numberType,
    });
    const sType = makeStructType('S', {
      's': s2Type,
    });

    const v = newStruct(sType, {s: newStruct(s2Type, {x: 42})});
    w.writeValue(v);
    assert.deepEqual([Kind.Struct, 'S',
      ['s', Kind.Struct, 'S2', ['x', Kind.Number]],
      Kind.Struct, 'S2', ['x', Kind.Number], Kind.Number, '42'], w.array);
  });

  test('write compound list', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const ltr = makeListType(numberType);
    const r1 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0])));
    const r2 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [1, 2])));
    const r3 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l = new NomsList(ltr, new IndexedMetaSequence(ds, ltr, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.List, Kind.Number, true, [
        Kind.Ref, Kind.List, Kind.Number, r1.targetRef.toString(), '1', Kind.Number, '1', '1',
        Kind.Ref, Kind.List, Kind.Number, r2.targetRef.toString(), '1', Kind.Number, '2', '2',
        Kind.Ref, Kind.List, Kind.Number, r3.targetRef.toString(), '1', Kind.Number, '3', '3',
      ],
    ], w.array);
  });

  test('write compound set with bool', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const str = makeSetType(boolType);
    const r1 = ds.writeValue(new NomsSet(str, new SetLeafSequence(ds, str, [true])));
    const r2 = ds.writeValue(new NomsSet(str, new SetLeafSequence(ds, str, [false])));
    const tuples = [
      new MetaTuple(r1, true, 1),
      new MetaTuple(r2, false, 1),
    ];
    const l = new NomsSet(str, new OrderedMetaSequence(ds, str, tuples));

    w.writeValue(l);
    assert.deepEqual([
      Kind.Set, Kind.Bool, true, [
        Kind.Ref, Kind.Set, Kind.Bool, r1.targetRef.toString(), '1', Kind.Bool, true, '1',
        Kind.Ref, Kind.Set, Kind.Bool, r2.targetRef.toString(), '1', Kind.Bool, false, '1',
      ],
    ], w.array);
  });

  test('write type value', async () => {
    const ds = new Database(makeTestingBatchStore());

    const test = (expected: Array<any>, v: Type) => {
      const w = new JsonArrayWriter(ds);
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

    const ds = new Database(makeTestingBatchStore());
    const blob = await newBlob(stringToUint8Array('hi'));

    const chunk = encodeNomsValue(blob, ds);
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
    const chunk2 = encodeNomsValue(blob2, ds);
    assert.equal(buffer2.byteLength, chunk2.data.buffer.byteLength);
    assert.deepEqual(buffer2, chunk2.data.buffer);
  });

  test('write ref', async () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const ref = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const t = makeRefType(blobType);
    const v = constructRefValue(t, ref, 1);
    w.writeValue(v);

    assert.deepEqual([Kind.Ref, Kind.Blob, ref.toString(), '1'], w.array);
  });

  test('write union list', () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const tr = makeListType(makeUnionType([stringType, numberType]));
    const v = new NomsList(tr, new ListLeafSequence(ds, tr, ['hi', 42]));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.Union, 2, Kind.Number, Kind.String,
      false, [Kind.String, 'hi', Kind.Number, '42']], w.array);
  });

  test('write empty union list', () => {
    const ds = new Database(makeTestingBatchStore());
    const w = new JsonArrayWriter(ds);
    const tr = makeListType(makeUnionType([]));
    const v = new NomsList(tr, new ListLeafSequence(ds, tr, []));
    w.writeValue(v);
    assert.deepEqual([Kind.List, Kind.Union, 0, false, []], w.array);
  });
});
