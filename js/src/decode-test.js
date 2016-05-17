// @flow

import Chunk from './chunk.js';
import Database from './database.js';
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
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  Type,
  typeType,
  valueType,
} from './type.js';
import {encode as encodeBase64} from './base64.js';
import {newListMetaSequence, MetaTuple, newSetMetaSequence} from './meta-sequence.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms-kind.js';
import {newListLeafSequence, NomsList} from './list.js';
import {newMapLeafSequence, NomsMap} from './map.js';
import {NomsBlob, newBlob} from './blob.js';
import {NomsSet, newSetLeafSequence} from './set.js';
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
    const ds = new Database(makeTestingBatchStore());
    const a = [1, 'hi', true];
    const r = new JsonArrayReader(a, ds);

    assert.strictEqual(1, r.read());
    assert.isFalse(r.atEnd());

    assert.strictEqual('hi', r.readString());
    assert.isFalse(r.atEnd());

    assert.strictEqual(true, r.readBool());
    assert.isTrue(r.atEnd());
  });

  test('read type', () => {
    const ds = new Database(makeTestingBatchStore());
    function doTest(expected: Type, a: Array<any>) {
      const r = new JsonArrayReader(a, ds);
      const tr = r.readValue();
      assert.isTrue(equals(expected, tr));
    }

    doTest(boolType, [Kind.Type, Kind.Bool, true]);
    doTest(typeType, [Kind.Type, Kind.Type, Kind.Bool]);
    doTest(makeListType(boolType), [Kind.Type, Kind.List, Kind.Bool, true, false]);
    doTest(makeStructType('S', {'x': boolType}), [Kind.Type, Kind.Struct, 'S', ['x', Kind.Bool]]);

    doTest(makeUnionType([]), [Kind.Type, Kind.Union, 0]);
    doTest(makeUnionType([numberType, stringType]),
           [Kind.Type, Kind.Union, 2, Kind.Number, Kind.String]);
    doTest(makeListType(makeUnionType([])), [Kind.Type, Kind.List, Kind.Union, 0]);
  });

  test('read primitives', () => {
    const ds = new Database(makeTestingBatchStore());

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
    const ds = new Database(makeTestingBatchStore());
    const a = [Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2', Kind.Number, '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<number> = r.readValue();
    invariant(v instanceof NomsList);

    const l = new NomsList(newListLeafSequence(ds, [0, 1, 2, 3]));
    assert.isTrue(equals(l, v));
  });

  test('read list of mixed types', async () => {
    const ds = new Database(makeTestingBatchStore());
    const a = [Kind.List, Kind.Union, 3, Kind.Bool, Kind.Number, Kind.String, false,
      [Kind.Number, '1', Kind.String, 'hi', Kind.Bool, true]];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<Value> = r.readValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(makeUnionType([boolType, numberType, stringType]));
    assert.isTrue(equals(v.type, tr));
    assert.strictEqual(1, await v.get(0));
    assert.strictEqual('hi', await v.get(1));
    assert.strictEqual(true, await v.get(2));
  });

  test('read set of mixed types', async () => {
    const ds = new Database(makeTestingBatchStore());
    const a = [Kind.Set, Kind.Union, 3, Kind.Bool, Kind.Number, Kind.String, false,
      [Kind.Bool, true, Kind.Number, '1', Kind.String, 'hi']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<boolean | number | string> = r.readValue();
    invariant(v instanceof NomsSet);

    const tr = makeSetType(makeUnionType([boolType, numberType, stringType]));
    assert.isTrue(equals(v.type, tr));
    assert.isTrue(await v.has(1));
    assert.isTrue(await v.has('hi'));
    assert.isTrue(await v.has(true));
  });

  test('read map of mixed types', async () => {
    const ds = new Database(makeTestingBatchStore());
    const a = [
      Kind.Map, Kind.Union, 2, Kind.Bool, Kind.Number,
      Kind.Union, 2, Kind.Number, Kind.String, false, [
        Kind.Bool, true, Kind.Number, '1',
        Kind.Number, '2', Kind.String, 'hi',
      ],
    ];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<boolean | number, number | string> = r.readValue();
    invariant(v instanceof NomsMap);

    const tr = makeMapType(makeUnionType([boolType, numberType]),
                           makeUnionType([numberType, stringType]));
    assert.isTrue(equals(v.type, tr));
    assert.equal(await v.get(true), 1);
    assert.equal(await v.get(2), 'hi');
  });

  test('read value list of number', () => {
    const ds = new Database(makeTestingBatchStore());
    const a = [Kind.Value, Kind.List, Kind.Number, false,
      [Kind.Number, '0', Kind.Number, '1', Kind.Number, '2']];
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsList);

    const l = new NomsList(newListLeafSequence(ds, [0, 1, 2]));
    assert.isTrue(equals(l, v));
  });

  test('read compound list', () => {
    const ds = new Database(makeTestingBatchStore());
    const r1 = ds.writeValue(new NomsList(newListLeafSequence(ds, [0])));
    const r2 = ds.writeValue(new NomsList(newListLeafSequence(ds, [1, 2])));
    const r3 = ds.writeValue(new NomsList(newListLeafSequence(ds, [3, 4, 5])));
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l: NomsList<number> = new NomsList(newListMetaSequence(ds, tuples));

    const a = [
      Kind.List, Kind.Number, true, [
        Kind.Ref, Kind.List, Kind.Number, r1.targetRef.toString(), '1', Kind.Number, '1', '1',
        Kind.Ref, Kind.List, Kind.Number, r2.targetRef.toString(), '1', Kind.Number, '2', '2',
        Kind.Ref, Kind.List, Kind.Number, r3.targetRef.toString(), '1', Kind.Number, '3', '3',
      ],
    ];
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsList);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read map of number to number', () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[MapKind, NumberKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);

    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const m = new NomsMap(newMapLeafSequence(ds, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(equals(v, m));
  });

  test('read map of ref to number', () => {
    const ds = new Database(makeTestingBatchStore());
    const rv1 = ds.writeValue(true);
    const rv2 = ds.writeValue('hi');
    const a = [
      Kind.Map, Kind.Union, 2, Kind.Ref, Kind.String, Kind.Ref, Kind.Bool, Kind.Number, false, [
        Kind.Ref, Kind.Bool, rv1.targetRef.toString(), '1', Kind.Number, '2',
        Kind.Ref, Kind.String, rv2.targetRef.toString(), '1', Kind.Number, '4',
      ],
    ];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<RefValue<Value>, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const m = new NomsMap(newMapLeafSequence(ds, [{key: rv1, value: 2}, {key: rv2, value: 4}]));
    assert.isTrue(equals(v, m));
  });

  test('read value map of number to number', () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[ValueKind, MapKind, NumberKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = r.readValue();
    invariant(v instanceof NomsMap);

    const m = new NomsMap(newMapLeafSequence(ds, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(equals(v, m));
  });

  test('read set of number', () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[SetKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = r.readValue();
    invariant(v instanceof NomsSet);

    const s = new NomsSet(newSetLeafSequence(ds, [0, 1, 2, 3]));
    assert.isTrue(equals(v, s));
  });

  test('read compound set', () => {
    const ds = new Database(makeTestingBatchStore());
    const r1 = ds.writeValue(new NomsSet(newSetLeafSequence(ds, [0, 1])));
    const r2 = ds.writeValue(new NomsSet(newSetLeafSequence(ds, [2, 3, 4])));
    const tuples = [
      new MetaTuple(r1, 1, 2),
      new MetaTuple(r2, 4, 3),
    ];
    const l: NomsSet<number> = new NomsSet(newSetMetaSequence(ds, tuples));

    const a = parseJson(`[
      SetKind, NumberKind, true, [
        RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "1", "2",
        RefKind, SetKind, NumberKind, "%s", "1", NumberKind, "4", "3"
      ]
    ]`, r1.targetRef.toString(), r2.targetRef.toString());
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    invariant(v instanceof NomsSet);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read value set of number', () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[ValueKind, SetKind, NumberKind, false,
      [NumberKind, "0", NumberKind, "1", NumberKind, "2", NumberKind, "3"]]`);
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = r.readValue();
    invariant(v instanceof NomsSet);

    const s = new NomsSet(newSetLeafSequence(ds, [0, 1, 2, 3]));
    assert.isTrue(equals(v, s));
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
    const ds = new Database(makeTestingBatchStore());
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
    const ds = new Database(makeTestingBatchStore());
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
      l: new NomsList(newListLeafSequence(ds, [0, 1, 2])),
      s: 'hi',
    });
  });

  test('test read value struct', () => {
    const ds = new Database(makeTestingBatchStore());
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
    const ds = new Database(makeTestingBatchStore());
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
    const ds = new Database(makeTestingBatchStore());
    const chunk = Chunk.fromString(
        `t [${Kind.Value},${Kind.Set},${Kind.Number},false,[${Kind.Number},"0",${
          Kind.Number},"1",${Kind.Number},"2",${Kind.Number},"3"]]`);
    const v = decodeNomsValue(chunk, new Database(makeTestingBatchStore()));
    invariant(v instanceof NomsSet);

    const s: NomsSet<number> = new NomsSet(newSetLeafSequence(ds, [0, 1, 2, 3]));

    assert.isTrue(equals(v, s));
  });

  test('decodeNomsValue: counter with one commit', async () => {
    const bs = makeTestingBatchStore();
    const ds = new Database(bs);

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
        ], commitRef.toString(), '1',
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
    const blob = decodeNomsValue(chunk, new Database(makeTestingBatchStore()));
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
    const blob2 = decodeNomsValue(chunk2, new Database(makeTestingBatchStore()));
    invariant(blob2 instanceof NomsBlob);
    const r2 = await blob2.getReader().read();
    assert.isFalse(r2.done);
    invariant(r2.value);
    assert.equal(bytes.length, r2.value.length);
    assert.deepEqual(bytes, r2.value);
  });

  test('inline blob', async () => {
    const ds = new Database(makeTestingBatchStore());
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
    const ds = new Database(makeTestingBatchStore());

    const r1 = ds.writeValue(await newBlob(stringToUint8Array('hi')));
    const r2 = ds.writeValue(await newBlob(stringToUint8Array('world')));

    const a = parseJson(`[
      BlobKind, true, [
        RefKind, BlobKind, "%s", "1", NumberKind, "2", "2",
        RefKind, BlobKind, "%s", "1", NumberKind, "5", "5"
      ]
    ]`, r1.targetRef, r2.targetRef);
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
    const db = new Database(makeTestingBatchStore());

    // struct A {
    //   b: struct B {
    //     a: List<A>
    //     b: List<B>
    //   }
    // }

    const at = makeStructType('A', {
      'b': valueType,  // placeholder
    });
    const bt = makeStructType('B', {
      'a': makeListType(at),
      'b': valueType,  // placeholder
    });
    at.desc.fields['b'] = bt;
    bt.desc.fields['b'] = makeListType(bt);

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

    const r = new JsonArrayReader(a, db);
    const v = r.readValue();

    assert.isTrue(equals(v.type, at));
    assert.isTrue(equals(v.b.type, bt));
  });

  test('read union list', async () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[ListKind, UnionKind, 2, StringKind, NumberKind,
      false, [StringKind, "hi", NumberKind, "42"]]`);
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    const v2 = new NomsList(newListLeafSequence(ds, ['hi', 42]));
    assert.isTrue(equals(v, v2));
  });

  test('read empty union list', async () => {
    const ds = new Database(makeTestingBatchStore());
    const a = parseJson(`[ListKind, UnionKind, 0, false, []]`);
    const r = new JsonArrayReader(a, ds);
    const v = r.readValue();
    const v2 = new NomsList(newListLeafSequence(ds, []));
    assert.isTrue(equals(v, v2));
  });
});
