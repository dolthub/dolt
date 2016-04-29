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
  makeCompoundType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  numberType,
  stringType,
  Type,
  valueType,
} from './type.js';
import {IndexedMetaSequence, MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import {Kind} from './noms-kind.js';
import {newList, ListLeafSequence, NomsList} from './list.js';
import {newMap, MapLeafSequence, NomsMap} from './map.js';
import {newSet, NomsSet, SetLeafSequence} from './set.js';
import {newBlob} from './blob.js';
import DataStore from './data-store.js';
import type {valueOrPrimitive} from './value.js';

suite('Encode', () => {
  test('write primitives', () => {
    function f(k: NomsKind, t:Type, v: valueOrPrimitive, ex: valueOrPrimitive) {
      const ms = new MemoryStore();
      const ds = new DataStore(ms);
      const w = new JsonArrayWriter(ds);
      w.writeTopLevel(t, v);
      assert.deepEqual([k, ex], w.array);
    }

    f(Kind.Bool, boolType, true, true);
    f(Kind.Bool, boolType, false, false);

    f(Kind.Number, numberType, 0, '0');

    f(Kind.Number, numberType, 1e18, '1000000000000000000');
    f(Kind.Number, numberType, 1e19, '10000000000000000000');
    f(Kind.Number, numberType, 1e20, '1e+20');

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

    const tr = makeCompoundType(Kind.List, numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, l);
    assert.deepEqual([Kind.List, Kind.Number, false, ['0', '1', '2', '3']], w.array);
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

    const it = makeCompoundType(Kind.List, numberType);
    const tr = makeCompoundType(Kind.List, it);
    const v = new NomsList(tr, new ListLeafSequence(ds, tr, [
      new NomsList(tr, new ListLeafSequence(ds, it, [0])),
      new NomsList(tr, new ListLeafSequence(ds, it, [1, 2, 3])),
    ]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.List, Kind.List, Kind.Number, false, [false, ['0'], false,
        ['1', '2', '3']]], w.array);
  });

  test('write leaf set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const tr = makeCompoundType(Kind.Set, numberType);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [0, 1, 2, 3]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Number, false, ['0', '1', '2', '3']], w.array);
  });

  test('write compound set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const ltr = makeCompoundType(Kind.Set, numberType);
    const r1 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [0]))).targetRef;
    const r2 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [1, 2]))).targetRef;
    const r3 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [3, 4, 5]))).targetRef;
    const tuples = [
      new MetaTuple(r1, 0, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 5, 3),
    ];
    const l = new NomsSet(ltr, new OrderedMetaSequence(ds, ltr, tuples));

    w.writeTopLevel(ltr, l);
    assert.deepEqual([Kind.Set, Kind.Number, true, [r1.toString(), '0', '1', r2.toString(), '2',
                     '2', r3.toString(), '5', '3']], w.array);
  });

  test('write set of set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const st = makeCompoundType(Kind.Set, numberType);
    const tr = makeCompoundType(Kind.Set, st);
    const v = new NomsSet(tr, new SetLeafSequence(ds, tr, [
      new NomsSet(tr, new SetLeafSequence(ds, st, [0])),
      new NomsSet(tr, new SetLeafSequence(ds, st, [1, 2, 3])),
    ]));

    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Set, Kind.Set, Kind.Number, false, [false, ['0'], false,
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

    const kt = makeCompoundType(Kind.Map, stringType, numberType);
    const vt = makeCompoundType(Kind.Set, boolType);
    const tr = makeCompoundType(Kind.Map, kt, vt);

    const s = new NomsSet(vt, new SetLeafSequence(ds, vt, [true]));
    const m1 = new NomsMap(kt, new MapLeafSequence(ds, kt, [{key: 'a', value: 0}]));
    const v = new NomsMap(kt, new MapLeafSequence(ds, tr, [{key: m1, value: s}]));
    w.writeTopLevel(tr, v);
    assert.deepEqual([Kind.Map, Kind.Map, Kind.String, Kind.Number, Kind.Set, Kind.Bool, false,
        [false, ['a', '0'], false, [true]]], w.array);
  });

  test('write empty struct', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const type = makeStructType('S', []);
    const v = newStruct(type, {});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Struct, 'S', []], w.array);
  });

  test('write struct', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const type = makeStructType('S', [
      new Field('x', numberType),
      new Field('b', boolType),
    ]);

    const v = newStruct(type, {x: 42, b: true});

    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Struct, 'S', ['x', Kind.Number, 'b', Kind.Bool], '42', true], w.array);
  });

  test('write struct with list', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    let w = new JsonArrayWriter(ds);

    const ltr = makeCompoundType(Kind.List, stringType);
    const type = makeStructType('S', [
      new Field('l', ltr),
    ]);

    let v = newStruct(type, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, ['a', 'b']))});
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.String], false, ['a', 'b']], w.array);

    v = newStruct(type, {l: new NomsList(ltr, new ListLeafSequence(ds, ltr, []))});
    w = new JsonArrayWriter(ds);
    w.writeTopLevel(type, v);
    assert.deepEqual([Kind.Struct, 'S', ['l', Kind.List, Kind.String], false, []], w.array);
  });

  test('write struct with struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);

    const s2Type = makeStructType('S2', [
      new Field('x', numberType),
    ]);
    const sType = makeStructType('S', [
      new Field('s', s2Type),
    ]);

    const v = newStruct(sType, {s: newStruct(s2Type, {x: 42})});
    w.writeTopLevel(sType, v);
    assert.deepEqual([Kind.Struct, 'S',
      ['s', Kind.Struct, 'S2', ['x', Kind.Number]], '42'], w.array);
  });

  test('write compound list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const w = new JsonArrayWriter(ds);
    const ltr = makeListType(numberType);
    const r1 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0]))).targetRef;
    const r2 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [1, 2]))).targetRef;
    const r3 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [3, 4, 5]))).targetRef;
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l = new NomsList(ltr, new IndexedMetaSequence(ds, ltr, tuples));

    w.writeTopLevel(ltr, l);
    assert.deepEqual([Kind.List, Kind.Number, true, [r1.toString(), '1', '1', r2.toString(), '2',
                     '2', r3.toString(), '3', '3']], w.array);
  });

  test('write type value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const test = (expected: Array<any>, v: Type) => {
      const w = new JsonArrayWriter(ds);
      w.writeTopLevel(v.type, v);
      assert.deepEqual(w.array, expected);
    };

    test([Kind.Type, Kind.Number], numberType);
    test([Kind.Type, Kind.List, [Kind.Bool]],
         makeCompoundType(Kind.List, boolType));
    test([Kind.Type, Kind.Map, [Kind.Bool, Kind.String]],
         makeCompoundType(Kind.Map, boolType, stringType));
    test([Kind.Type, Kind.Struct, 'S', ['x', Kind.Number, 'v', Kind.Value]],
         makeStructType('S', [
           new Field('x', numberType),
           new Field('v', valueType),
         ]));

    // struct A6 {
    //   v: Number
    //   cs: List<A6>
    // }

    const st = makeStructType('A6', [
      new Field('v', numberType),
      new Field('cs', valueType /* placeholder */),
    ]);
    const lt = makeListType(st);
    st.desc.fields[1].t = lt;

    test([Kind.Type, Kind.Struct, 'A6', ['v', Kind.Number, 'cs', Kind.List, Kind.Parent, 0]], st);
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

    test('Number', 'String', numberType, 'hi');
    test('Bool', 'String', boolType, 'hi');
    test('Blob', 'String', blobType, 'hi');

    test('String', 'Number', stringType, 42);
    test('Bool', 'Number', boolType, 42);
    test('Blob', 'Number', blobType, 42);

    test('Number', 'Bool', numberType, true);
    test('String', 'Bool', stringType, true);
    test('Blob', 'Bool', blobType, true);

    const blob = await newBlob(new Uint8Array([0, 1]));
    test('Number', 'Blob', numberType, blob);
    test('String', 'Blob', stringType, blob);
    test('Bool', 'Blob', boolType, blob);

    const list = await newList([0, 1], makeListType(numberType));
    test('Number', 'List<Number>', numberType, list);
    test('String', 'List<Number>', stringType, list);
    test('Bool', 'List<Number>', boolType, list);
    test('Blob', 'List<Number>', blobType, list);

    const map = await newMap(['zero', 1], makeMapType(stringType, numberType));
    test('Number', 'Map<String, Number>', numberType, map);
    test('String', 'Map<String, Number>', stringType, map);
    test('Bool', 'Map<String, Number>', boolType, map);
    test('Blob', 'Map<String, Number>', blobType, map);

    const set = await newSet([0, 1], makeSetType(numberType));
    test('Number', 'Set<Number>', numberType, set);
    test('String', 'Set<Number>', stringType, set);
    test('Bool', 'Set<Number>', boolType, set);
    test('Blob', 'Set<Number>', blobType, set);
  });
});
