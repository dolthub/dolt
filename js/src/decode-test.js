// @flow

import Chunk from './chunk.js';
import DataStore from './data-store.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';
import type RefValue from './ref-value.js';
import {default as Struct, StructMirror} from './struct.js';
import type {TypeDesc} from './type.js';
import type {Value} from './value.js';
import {assert} from 'chai';
import {decodeNomsValue, JsonArrayReader} from './decode.js';
import {
  boolType,
  makeListType,
  makeMapType,
  makeRefType,
  makeSetType,
  makeStructType,
  numberType,
  stringType,
  Type,
  typeType,
  valueType,
} from './type.js';
import {encode as encodeBase64} from './base64.js';
import {IndexedMetaSequence, MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsBlob, newBlob} from './blob.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {suite, test} from 'mocha';
import {equals} from './compare.js';

suite('Decode', () => {
  function stringToUint8Array(s): Uint8Array {
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
      bytes[i] = s.charCodeAt(i);
    }
    return bytes;
  }

  function parseJson(s, ...replacements) {
    s = s.replace(/\w+Kind/g, word => String(Kind[word.slice(0, -4)]));

    let i = 0;
    s = s.replace(/%s/g, () => String(replacements[i++]));
    return JSON.parse(s);
  }

  test('read', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = [1, 'hi', true];
    const r = new JsonArrayReader(a, ds);

    assert.strictEqual(1, r.read());
    assert.isFalse(r.atEnd());

    assert.strictEqual('hi', r.readString());
    assert.isFalse(r.atEnd());

    assert.strictEqual(true, r.readBool());
    assert.isTrue(r.atEnd());
  });

  test('read type as tag', () => {
    const ds = new DataStore(makeTestingBatchStore());
    function doTest(expected: Type, a: Array<any>) {
      const r = new JsonArrayReader(a, ds);
      const tr = r.readTypeAsTag([]);
      assert.isTrue(expected.equals(tr));
    }

    doTest(boolType, [Kind.Bool, true]);
    doTest(typeType, [Kind.Type, Kind.Bool]);
    doTest(makeListType(boolType), [Kind.List, Kind.Bool, true, false]);
    doTest(makeStructType('S', {'x': boolType}), [Kind.Struct, 'S', ['x', Kind.Bool]]);
  });

  test('read primitives', () => {
    const ds = new DataStore(makeTestingBatchStore());

    function doTest(expected: any, a: Array<any>): void {
      const r = new JsonArrayReader(a, ds);
      const v = r.readValue();
      assert.deepEqual(expected, v);
    }

    doTest(true, [Kind.Bool, true]);
    doTest(false, [Kind.Bool, false]);
    doTest(0, [Kind.Number, '0']);

    doTest(1e18, [Kind.Number, '1000000000000000000']);
    doTest(1e19, [Kind.Number, '10000000000000000000']);
    doTest(1e20, [Kind.Number, '1e+20']);

    doTest('hi', [Kind.String, 'hi']);
  });

  test('read list of number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = [Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<number> = r.readValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    assert.isTrue(l.equals(v));
  });

  // TODO: Can't round-trip collections of value types. =-(
  test('read list of value', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = [Kind.List, Kind.Value, false,
      [Kind.Number, '1', Kind.String, 'hi', Kind.Bool, true]];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<Value> = r.readValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(valueType);
    assert.isTrue(v.type.equals(tr));
    assert.strictEqual(1, await v.get(0));
    assert.strictEqual('hi', await v.get(1));
    assert.strictEqual(true, await v.get(2));
  });

  test('read value list of number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = [Kind.Value, Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2']];
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2]));
    assert.isTrue(l.equals(v));
  });

  test('read compound list', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const ltr = makeListType(numberType);
    const r1 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0])));
    const r2 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [1, 2])));
    const r3 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l: NomsList<number> = new NomsList(ltr, new IndexedMetaSequence(ds, ltr, tuples));

    const a = [Kind.List, Kind.Number, true,
               [r1.targetRef.toString(), Kind.Number, '1', '1',
                r2.targetRef.toString(), Kind.Number, '2', '2',
                r3.targetRef.toString(), Kind.Number, '3', '3']];
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsList);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read map of number to number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = parseJson(`[MapKind, NumberKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);

    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const t = makeMapType(numberType, numberType);
    const m = new NomsMap(t, new MapLeafSequence(ds, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read map of ref to number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const rv1 = ds.writeValue(true);
    const rv2 = ds.writeValue('hi');
    const a = [
      Kind.Map, Kind.Ref, Kind.Value, Kind.Number, false, [
        Kind.Ref, Kind.Bool, rv1.targetRef.toString(), Kind.Number, '2',
        Kind.Ref, Kind.String, rv2.targetRef.toString(), Kind.Number, '4',
      ],
    ];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<RefValue<Value>, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const refOfValueType = makeRefType(valueType);
    const mapType = makeMapType(refOfValueType, numberType);

    const m = new NomsMap(mapType, new MapLeafSequence(ds, mapType, [{key: rv1, value: 2},
                                                                     {key: rv2, value: 4}]));
    assert.isTrue(v.equals(m));
  });

  test('read value map of number to number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = parseJson(`[ValueKind, MapKind, NumberKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const t = makeMapType(numberType, numberType);
    const m = new NomsMap(t, new MapLeafSequence(ds, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read set of number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = parseJson(`[SetKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = r.readValue();
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  test('read compound set', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const ltr = makeSetType(numberType);
    const r1 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [0, 1])));
    const r2 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [2, 3, 4])));
    const tuples = [
      new MetaTuple(r1, 1, 2),
      new MetaTuple(r2, 4, 3),
    ];
    const l: NomsSet<number> = new NomsSet(ltr, new OrderedMetaSequence(ds, ltr, tuples));

    const a = parseJson(`[SetKind, NumberKind, true,
      ["%s", NumberKind, "1", "2", "%s", NumberKind, "4", "3"]]`,
      r1.targetRef.toString(), r2.targetRef.toString());
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsSet);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read value set of number', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = parseJson(`[ValueKind, SetKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = r.readValue();
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  function assertStruct(s: ?Struct, desc: TypeDesc, data: {[key: string]: any}) {
    notNull(s);
    invariant(s instanceof Struct, 'expected instanceof struct');
    const mirror = new StructMirror(s);
    assert.isTrue(desc.equals(mirror.desc));

    for (const key in data) {
      assert.isTrue(equals(data[key], notNull(mirror.get(key))));
    }
  }

  test('test read struct', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeStructType('A1', {
      'x': numberType,
      's': stringType,
      'b': boolType,
    });

    const a = parseJson(`[StructKind, "A1", ["b", BoolKind, "s", StringKind, "x", NumberKind],
      BoolKind, true, StringKind, "hi", NumberKind, "42"]`);
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read struct with list', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const ltr = makeListType(numberType);
    const tr = makeStructType('A4', {
      'b': boolType,
      'l': ltr,
      's': stringType,
    });

    const a = parseJson(`[
      StructKind, "A4", [
        "b", BoolKind,
        "l", ListKind, NumberKind,
        "s", StringKind
      ],
      BoolKind, true,
      ListKind, NumberKind, false, [
        NumberKind, "0",
        NumberKind, "1",
        NumberKind, "2"
      ],
      StringKind, "hi"]`);
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();

    assertStruct(v, tr.desc, {
      b: true,
      l: new NomsList(ltr, new ListLeafSequence(ds, ltr, [0, 1, 2])),
      s: 'hi',
    });
  });

  test('test read struct with value', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeStructType('A5', {
      'b': boolType,
      'v': valueType,
      's': stringType,
    });

    const a = parseJson(`[
      StructKind, "A5", [
        "b", BoolKind,
        "s", StringKind,
        "v", ValueKind
      ],
      BoolKind, true,
      StringKind, "hi",
      NumberKind, "42"
    ]`);

    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();

    assertStruct(v, tr.desc, {
      b: true,
      s: 'hi',
      v: 42,
    });
  });

  test('test read value struct', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeStructType('A1', {
      'x': numberType,
      's': stringType,
      'b': boolType,
    });

    const a = parseJson(`[
      ValueKind, StructKind, "A1", [
        "b", BoolKind,
        "s", StringKind,
        "x", NumberKind
      ],
      BoolKind, true,
      StringKind, "hi",
      NumberKind, "42"
    ]`);

    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read map of string to struct', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeStructType('s', {
      'b': boolType,
      'i': numberType,
    });

    const a = parseJson(`[
      MapKind, StringKind, StructKind, "s", ["b", BoolKind, "i", NumberKind], false, [
        StringKind, "bar", StructKind, "s", ["b", BoolKind, "i", NumberKind],
          BoolKind, false, NumberKind, "2",
        StringKind, "baz", StructKind, "s", ["b", BoolKind, "i", NumberKind],
          BoolKind, false, NumberKind, "1",
        StringKind, "foo", StructKind, "s", ["b", BoolKind, "i", NumberKind],
          BoolKind, true, NumberKind, "3"
      ]
    ]`);

    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<string, Struct> = r.readValue();
    invariant(v instanceof NomsMap);

    assert.strictEqual(3, v.size);
    assertStruct(await v.get('foo'), tr.desc, {b: true, i: 3});
    assertStruct(await v.get('bar'), tr.desc, {b: false, i: 2});
    assertStruct(await v.get('baz'), tr.desc, {b: false, i: 1});
  });

  test('decodeNomsValue', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const chunk = Chunk.fromString(
        `t [${Kind.Value},${Kind.Set},${Kind.Number},false,[${Kind.Number},"0",${
          Kind.Number},"1",${Kind.Number},"2",${Kind.Number},"3"]]`);
    const v = decodeNomsValue(chunk, new DataStore(makeTestingBatchStore()));
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s: NomsSet<number> = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));

    assert.isTrue(v.equals(s));
  });

  test('decodeNomsValue: counter with one commit', async () => {
    const bs = makeTestingBatchStore();
    const ds = new DataStore(bs);

    const makeChunk = a => Chunk.fromString(`t ${JSON.stringify(a)}`);

    // struct Commit {
    //   value: Value
    //   parents: Set<Ref<Commit>>
    // }

    // Commit value
    const commitChunk = makeChunk([
      Kind.Struct, 'Commit', [
        'value', Kind.Value,
        'parents', Kind.Set, Kind.Ref, Kind.Parent, 0,
      ],
      Kind.Set, Kind.Ref, Kind.Struct, 'Commit', [
        'value', Kind.Value,
        'parents', Kind.Set, Kind.Ref, Kind.Parent, 0,
      ], false, [],
      Kind.Number, '1']);
    const commitRef = commitChunk.ref;
    bs.schedulePut(commitChunk, new Set());

    // Root
    const rootChunk = makeChunk([
      Kind.Map, Kind.String, Kind.Ref, Kind.Struct, 'Commit', [
        'parents', Kind.Set, Kind.Ref, Kind.Parent, 0,
        'value', Kind.Value,
      ],
      false, [
        Kind.String, 'counter',
        Kind.Ref, Kind.Struct, 'Commit', [
          'parents', Kind.Set, Kind.Ref, Kind.Parent, 0,
          'value', Kind.Value,
        ], commitRef.toString(),
      ],
    ]);
    const rootRef = rootChunk.ref;
    bs.schedulePut(rootChunk, new Set());

    await bs.flush();
    const rootMap = await ds.readValue(rootRef);
    const counterRef = await rootMap.get('counter');
    const commit = await counterRef.targetValue(ds);
    assert.strictEqual(1, await commit.value);
  });

  test('out of line blob', async () => {
    const chunk = Chunk.fromString('b hi');
    const blob = decodeNomsValue(chunk, new DataStore(makeTestingBatchStore()));
    invariant(blob instanceof NomsBlob);
    const r = await blob.getReader().read();
    assert.isFalse(r.done);
    invariant(r.value);
    assert.equal(2, r.value.byteLength);
    assert.deepEqual(stringToUint8Array('hi'), r.value);

    const data = new Uint8Array(2 + 256);
    data[0] = 'b'.charCodeAt(0);
    data[1] = ' '.charCodeAt(0);
    const bytes = new Uint8Array(256);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
      data[2 + i] = i;
    }

    const chunk2 = new Chunk(data);
    const blob2 = decodeNomsValue(chunk2, new DataStore(makeTestingBatchStore()));
    invariant(blob2 instanceof NomsBlob);
    const r2 = await blob2.getReader().read();
    assert.isFalse(r2.done);
    invariant(r2.value);
    assert.equal(bytes.length, r2.value.length);
    assert.deepEqual(bytes, r2.value);
  });

  test('inline blob', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const a = parseJson(`[
      ListKind, BlobKind, false, [BlobKind, false, "%s", BlobKind, false, "%s"]
    ]`, encodeBase64(stringToUint8Array('hello')), encodeBase64(stringToUint8Array('world')));
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<NomsBlob> = r.readValue();
    invariant(v instanceof NomsList);

    assert.strictEqual(2, v.length);
    const [b1, b2] = [await v.get(0), await v.get(1)];
    assert.deepEqual({done: false, value: stringToUint8Array('hello')},
                     await b1.getReader().read());
    assert.deepEqual({done: false, value: stringToUint8Array('world')},
                     await b2.getReader().read());
  });

  test('compound blob', async () => {
    const ds = new DataStore(makeTestingBatchStore());

    const r1 = ds.writeValue(await newBlob(stringToUint8Array('hi')));
    const r2 = ds.writeValue(await newBlob(stringToUint8Array('world')));

    const a = parseJson(`[BlobKind, true, [
      "%s", NumberKind, "2", "2",
      "%s", NumberKind, "5", "5"]]`, r1.targetRef, r2.targetRef);
    const r = new JsonArrayReader(a, ds);
    const v: NomsBlob = r.readValue();
    invariant(v instanceof NomsBlob);

    const reader = v.getReader();
    assert.deepEqual(await reader.read(), {done: false, value: stringToUint8Array('hi')});
    const x = await reader.read();
    assert.deepEqual(x, {done: false, value: stringToUint8Array('world')});
    assert.deepEqual(await reader.read(), {done: true});
  });

  test('recursive struct', () => {
    const ds = new DataStore(makeTestingBatchStore());

    // struct A {
    //   b: struct B {
    //     a: List<A>
    //     b: List<B>
    //   }
    // }

    const ta = makeStructType('A', {
      'b': valueType,  // placeholder
    });
    const tb = makeStructType('B', {
      'a': valueType,  // placeholder
      'b': valueType,  // placeholder
    });
    ta.desc.fields['b'] = tb;
    tb.desc.fields['a'] = makeListType(ta);
    tb.desc.fields['b'] = makeListType(tb);

    const a = parseJson(`[
      StructKind, "A", [
        "b", StructKind, "B", [
          "a", ListKind, ParentKind, 1,
          "b", ListKind, ParentKind, 0
        ]
      ],
      StructKind, "B", [
        "a", ListKind, StructKind, "A", [
          "b", ParentKind, 1
        ],
        "b", ListKind, ParentKind, 0
      ],
      ListKind, StructKind, "A", [
        "b", StructKind, "B", [
          "a", ListKind, ParentKind, 1,
          "b", ListKind, ParentKind, 0
        ]
      ], false, [],
      ListKind, StructKind, "B", [
        "a", ListKind, StructKind, "A", [
          "b", ParentKind, 1
        ],
        "b", ListKind, ParentKind, 0
      ], false, []]`);
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();

    assert.isTrue(v.type.equals(ta));
    assert.isTrue(v.b.type.equals(tb));
  });
});
