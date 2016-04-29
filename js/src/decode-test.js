// @flow

import Chunk from './chunk.js';
import DataStore from './data-store.js';
import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import RefValue from './ref-value.js';
import {default as Struct, StructMirror} from './struct.js';
import type {TypeDesc} from './type.js';
import type {Value} from './value.js';
import {assert} from 'chai';
import {decodeNomsValue, JsonArrayReader} from './decode.js';
import {
  boolType,
  Field,
  makeStructType,
  makeListType,
  makeMapType,
  makeSetType,
  makeRefType,
  numberType,
  stringType,
  Type,
  typeType,
  valueType,
  StructDesc,
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

suite('Decode', () => {
  function stringToUint8Array(s): Uint8Array {
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
      bytes[i] = s.charCodeAt(i);
    }
    return bytes;
  }

  test('read', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [1, 'hi', true];
    const r = new JsonArrayReader(a, ds);

    assert.strictEqual(1, r.read());
    assert.isFalse(r.atEnd());

    assert.strictEqual('hi', r.readString());
    assert.isFalse(r.atEnd());

    assert.strictEqual(true, r.readBool());
    assert.isTrue(r.atEnd());
  });

  test('read type as tag', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    function doTest(expected: Type, a: Array<any>) {
      const r = new JsonArrayReader(a, ds);
      const tr = r.readTypeAsTag([]);
      assert.isTrue(expected.equals(tr));
    }

    doTest(boolType, [Kind.Bool, true]);
    doTest(typeType, [Kind.Type, Kind.Bool]);
    doTest(makeListType(boolType), [Kind.List, Kind.Bool, true, false]);
    doTest(makeStructType('S', [new Field('x', boolType)]), [Kind.Struct, 'S', ['x', Kind.Bool]]);
  });

  test('read primitives', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    async function doTest(expected: any, a: Array<any>): Promise<void> {
      const r = new JsonArrayReader(a, ds);
      const v = await r.readTopLevelValue();
      assert.deepEqual(expected, v);
    }

    await doTest(true, [Kind.Bool, true]);
    await doTest(false, [Kind.Bool, false]);
    await doTest(0, [Kind.Number, '0']);

    await doTest(1e18, [Kind.Number, '1000000000000000000']);
    await doTest(1e19, [Kind.Number, '10000000000000000000']);
    await doTest(1e20, [Kind.Number, '1e+20']);

    await doTest('hi', [Kind.String, 'hi']);
  });

  test('read list of number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.List, Kind.Number, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<number> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2, 3]));
    assert.isTrue(l.equals(v));
  });

  // TODO: Can't round-trip collections of value types. =-(
  test('read list of value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.List, Kind.Value, false,
      [Kind.Number, '1', Kind.String, 'hi', Kind.Bool, true]];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<Value> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(valueType);
    assert.isTrue(v.type.equals(tr));
    assert.strictEqual(1, await v.get(0));
    assert.strictEqual('hi', await v.get(1));
    assert.strictEqual(true, await v.get(2));
  });

  test('read value list of number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Value, Kind.List, Kind.Number, false, ['0', '1', '2']];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    const tr = makeListType(numberType);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [0, 1, 2]));
    assert.isTrue(l.equals(v));
  });

  test('read compound list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const ltr = makeListType(numberType);
    const r1 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [0]))).targetRef;
    const r2 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [1, 2]))).targetRef;
    const r3 = ds.writeValue(new NomsList(ltr, new ListLeafSequence(ds, ltr, [3, 4, 5]))).targetRef;
    const tuples = [
      new MetaTuple(r1, 1, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 3, 3),
    ];
    const l:NomsList<number> = new NomsList(ltr, new IndexedMetaSequence(ds, ltr, tuples));

    const a = [Kind.List, Kind.Number, true,
               [r1.toString(), '1', '1', r2.toString(), '2', '2', r3.toString(), '3', '3']];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();
    invariant(v instanceof NomsList);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read map of number to number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Map, Kind.Number, Kind.Number, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const t = makeMapType(numberType, numberType);
    const m = new NomsMap(t, new MapLeafSequence(ds, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read map of ref to number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Map, Kind.Ref, Kind.Value, Kind.Number, false,
               ['sha1-0000000000000000000000000000000000000001', '2',
                'sha1-0000000000000000000000000000000000000002', '4']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<RefValue<Value>, number> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const refOfValueType = makeRefType(valueType);
    const mapType = makeMapType(refOfValueType, numberType);
    const rv1 = new RefValue(new Ref('sha1-0000000000000000000000000000000000000001'),
        refOfValueType);
    const rv2 = new RefValue(new Ref('sha1-0000000000000000000000000000000000000002'),
        refOfValueType);
    const m = new NomsMap(mapType, new MapLeafSequence(ds, mapType, [{key: rv1, value: 2},
                                                                     {key: rv2, value: 4}]));
    assert.isTrue(v.equals(m));
  });

  test('read value map of number to number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Value, Kind.Map, Kind.Number, Kind.Number, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<number, number> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    const t = makeMapType(numberType, numberType);
    const m = new NomsMap(t, new MapLeafSequence(ds, t, [{key: 0, value: 1}, {key: 2, value: 3}]));
    assert.isTrue(v.equals(m));
  });

  test('read set of number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Set, Kind.Number, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = await r.readTopLevelValue();
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  test('read compound set', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const ltr = makeSetType(numberType);
    const r1 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [0]))).targetRef;
    const r2 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [1, 2]))).targetRef;
    const r3 = ds.writeValue(new NomsSet(ltr, new SetLeafSequence(ds, ltr, [3, 4, 5]))).targetRef;
    const tuples = [
      new MetaTuple(r1, 0, 1),
      new MetaTuple(r2, 2, 2),
      new MetaTuple(r3, 5, 3),
    ];
    const l:NomsSet<number> = new NomsSet(ltr, new OrderedMetaSequence(ds, ltr, tuples));

    const a = [Kind.Set, Kind.Number, true,
               [r1.toString(), '0', '1', r2.toString(), '2', '2', r3.toString(), '5', '3']];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();
    invariant(v instanceof NomsSet);
    assert.isTrue(v.ref.equals(l.ref));
  });

  test('read value set of number', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [Kind.Value, Kind.Set, Kind.Number, false, ['0', '1', '2', '3']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsSet<number> = await r.readTopLevelValue();
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  function assertStruct(s: ?Struct, desc: TypeDesc, data: {[key: string]: any}) {
    notNull(s);
    invariant(s instanceof Struct, 'expected instanceof struct');
    const mirror = new StructMirror(s);
    assert.deepEqual(desc, mirror.desc);

    for (const key in data) {
      assert.deepEqual(data[key], mirror.get(key));
    }
  }

  test('test read struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeStructType('A1', [
      new Field('x', numberType),
      new Field('s', stringType),
      new Field('b', boolType),
    ]);

    const a = [Kind.Struct, 'A1', [
      'x', Kind.Number,
      's', Kind.String,
      'b', Kind.Bool,
    ], '42', 'hi', true];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read struct with list', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const ltr = makeListType(numberType);
    const tr = makeStructType('A4', [
      new Field('b', boolType),
      new Field('l', ltr),
      new Field('s', stringType),
    ]);

    const a = [Kind.Struct, 'A4', [
      'b', Kind.Bool,
      'l', Kind.List, Kind.Number,
      's', Kind.String,
    ], true, false, ['0', '1', '2'], 'hi'];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      l: new NomsList(ltr, new ListLeafSequence(ds, ltr, [0, 1, 2])),
      s: 'hi',
    });
  });

  test('test read struct with value', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeStructType('A5', [
      new Field('b', boolType),
      new Field('v', valueType),
      new Field('s', stringType),
    ]);

    const a = [Kind.Struct, 'A5', ['b', Kind.Bool, 'v', Kind.Value, 's', Kind.String],
      true, Kind.Number, '42', 'hi'];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      v: 42,
      s: 'hi',
    });
  });

  test('test read value struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeStructType('A1', [
      new Field('x', numberType),
      new Field('s', stringType),
      new Field('b', boolType),
    ]);

    const a = [Kind.Value, Kind.Struct, 'A1', ['x', Kind.Number, 's', Kind.String, 'b', Kind.Bool],
        '42', 'hi', true];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true,
    });
  });

  test('test read map of string to struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeStructType('s', [
      new Field('b', boolType),
      new Field('i', numberType),
    ]);

    const a = [Kind.Value, Kind.Map, Kind.String,
      Kind.Struct, 's', ['b', Kind.Bool, 'i', Kind.Number],
      false, ['bar', false, '2', 'baz', false, '1', 'foo', true, '3']];

    const r = new JsonArrayReader(a, ds);
    const v: NomsMap<string, Struct> = await r.readTopLevelValue();
    invariant(v instanceof NomsMap);

    assert.strictEqual(3, v.size);
    assertStruct(await v.get('foo'), tr.desc, {b: true, i: 3});
    assertStruct(await v.get('bar'), tr.desc, {b: false, i: 2});
    assertStruct(await v.get('baz'), tr.desc, {b: false, i: 1});
  });

  test('decodeNomsValue', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const chunk = Chunk.fromString(
        `t [${Kind.Value}, ${Kind.Set}, ${Kind.Number}, false, ["0", "1", "2", "3"]]`);
    const v = decodeNomsValue(chunk, new DataStore(new MemoryStore()));
    invariant(v instanceof NomsSet);

    const t = makeSetType(numberType);
    const s: NomsSet<number> = new NomsSet(t, new SetLeafSequence(ds, t, [0, 1, 2, 3]));
    assert.isTrue(v.equals(s));
  });

  test('decodeNomsValue: counter with one commit', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const makeChunk = a => Chunk.fromString(`t ${JSON.stringify(a)}`);

    // struct Commit {
    //   value: Value
    //   parents: Set<Ref<Commit>>
    // }

    // Commit value
    const commitChunk = makeChunk(
      [Kind.Struct, 'Commit',
        ['value', Kind.Value, 'parents', Kind.Set, Kind.Ref, Kind.Parent, 0],
        Kind.Number, '1', false, []]);
    const commitRef = commitChunk.ref;
    ms.put(commitChunk);

    // Root
    const rootChunk = makeChunk([Kind.Map, Kind.String, Kind.Ref, Kind.Struct, 'Commit',
      ['value', Kind.Value, 'parents', Kind.Set, Kind.Ref, Kind.Parent, 0],
      false, ['counter', commitRef.toString()]]);
    const rootRef = rootChunk.ref;
    ms.put(rootChunk);

    const rootMap = await ds.readValue(rootRef);
    const counterRef = await rootMap.get('counter');
    const commit = await counterRef.targetValue(ds);
    assert.strictEqual(1, await commit.value);
  });

  test('out of line blob', async () => {
    const chunk = Chunk.fromString('b hi');
    const blob = decodeNomsValue(chunk, new DataStore(new MemoryStore()));
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
    const blob2 = decodeNomsValue(chunk2, new DataStore(new MemoryStore()));
    invariant(blob2 instanceof NomsBlob);
    const r2 = await blob2.getReader().read();
    assert.isFalse(r2.done);
    invariant(r2.value);
    assert.equal(bytes.length, r2.value.length);
    assert.deepEqual(bytes, r2.value);
  });

  test('inline blob', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const a = [
      Kind.List, Kind.Blob, false,
      [false, encodeBase64(stringToUint8Array('hello')),
       false, encodeBase64(stringToUint8Array('world'))],
    ];
    const r = new JsonArrayReader(a, ds);
    const v: NomsList<NomsBlob> = await r.readTopLevelValue();
    invariant(v instanceof NomsList);

    assert.strictEqual(2, v.length);
    const [b1, b2] = [await v.get(0), await v.get(1)];
    assert.deepEqual({done: false, value: stringToUint8Array('hello')},
                     await b1.getReader().read());
    assert.deepEqual({done: false, value: stringToUint8Array('world')},
                     await b2.getReader().read());
  });

  test('compound blob', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const r1 = ds.writeValue(await newBlob(stringToUint8Array('hi'))).targetRef;
    const r2 = ds.writeValue(await newBlob(stringToUint8Array('world'))).targetRef;

    const a = [Kind.Blob, true, [r1.ref.toString(), '2', '2', r2.ref.toString(), '5', '5']];
    const r = new JsonArrayReader(a, ds);
    const v: NomsBlob = await r.readTopLevelValue();
    invariant(v instanceof NomsBlob);

    const reader = v.getReader();
    assert.deepEqual(await reader.read(), {done: false, value: stringToUint8Array('hi')});
    const x = await reader.read();
    assert.deepEqual(x, {done: false, value: stringToUint8Array('world')});
    assert.deepEqual(await reader.read(), {done: true});
  });

  test('recursive struct', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    // struct A {
    //   b: struct B {
    //     a: List<A>
    //     b: List<B>
    //   }
    // }

    const ta = makeStructType('A', []);
    const tb = makeStructType('B', []);
    invariant(ta.desc instanceof StructDesc);
    ta.desc.fields.push(new Field('b', tb));

    invariant(tb.desc instanceof StructDesc);
    const {fields} = tb.desc;
    fields.push(new Field('a', makeListType(ta)), new Field('b', makeListType(tb)));

    const a = [Kind.Struct, 'A',
          ['b', Kind.Struct, 'B', [
            'a', Kind.List, Kind.Parent, 1,
            'b', Kind.List, Kind.Parent, 0,
          ]], false, [], false, []];
    const r = new JsonArrayReader(a, ds);
    const v = await r.readTopLevelValue();

    assert.isTrue(v.type.equals(ta));
    assert.isTrue(v.b.type.equals(tb));
  });
});
