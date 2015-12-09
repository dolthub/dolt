// @flow

import Chunk from './chunk.js';
import CompoundList from './compound_list.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import test from './async_test.js';
import type {TypeDesc} from './type.js';
import {assert} from 'chai';
import {decodeNomsValue, JsonArrayReader, readValue} from './decode.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, Type} from './type.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {MetaTuple} from './meta_sequence.js';
import {registerPackage, Package} from './package.js';
import {suite} from 'mocha';
import {writeValue} from './encode.js';

suite('Decode', () => {
  test('read', async () => {
    let ms = new MemoryStore();
    let a = [1, 'hi', true];
    let r = new JsonArrayReader(a, ms);

    assert.strictEqual(1, r.read());
    assert.isFalse(r.atEnd());

    assert.strictEqual('hi', r.readString());
    assert.isFalse(r.atEnd());

    assert.strictEqual(true, r.readBool());
    assert.isTrue(r.atEnd());
  });

  test('read type as tag', async () => {
    let ms = new MemoryStore();

    function doTest(expected: Type, a: Array<any>) {
      let r = new JsonArrayReader(a, ms);
      let tr = r.readTypeAsTag();
      assert.isTrue(expected.equals(tr));
    }

    doTest(makePrimitiveType(Kind.Bool), [Kind.Bool, true]);
    doTest(makePrimitiveType(Kind.Type), [Kind.Type, Kind.Bool]);
    doTest(makeCompoundType(Kind.List, makePrimitiveType(Kind.Bool)), [Kind.List, Kind.Bool, true, false]);

    let pkgRef = Ref.parse('sha1-a9993e364706816aba3e25717850c26c9cd0d89d');
    doTest(makeType(pkgRef, 42), [Kind.Unresolved, pkgRef.toString(), 42]);

    doTest(makePrimitiveType(Kind.Type), [Kind.Type, Kind.Type, pkgRef.toString()]);
  });

  test('read primitives', async () => {
    let ms = new MemoryStore();

    async function doTest(expected: any, a: Array<any>): Promise<void> {
      let r = new JsonArrayReader(a, ms);
      let v = await r.readTopLevelValue();
      assert.deepEqual(expected, v);
    }

    await doTest(true, [Kind.Bool, true]);
    await doTest(false, [Kind.Bool, false]);
    await doTest(0, [Kind.Uint8, 0]);
    await doTest(0, [Kind.Uint16, 0]);
    await doTest(0, [Kind.Uint32, 0]);
    await doTest(0, [Kind.Uint64, 0]);
    await doTest(0, [Kind.Int8, 0]);
    await doTest(0, [Kind.Int16, 0]);
    await doTest(0, [Kind.Int32, 0]);
    await doTest(0, [Kind.Int64, 0]);
    await doTest(0, [Kind.Float32, 0]);
    await doTest(0, [Kind.Float64, 0]);

    await doTest('hi', [Kind.String, 'hi']);

    let blob = new Uint8Array([0x00, 0x01]).buffer;
    await doTest(blob, [Kind.Blob, false, 'AAE=']);
  });

  test('read list of int 32', async () => {
    let ms = new MemoryStore();
    let a = [Kind.List, Kind.Int32, false, [0, 1, 2, 3]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();
    assert.deepEqual([0, 1, 2, 3], v);
  });

  test('read list of value', async () => {
    let ms = new MemoryStore();
    let a = [Kind.List, Kind.Value, false, [Kind.Int32, 1, Kind.String, 'hi', Kind.Bool, true]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();
    assert.deepEqual([1, 'hi', true], v);
  });

  test('read value list of int8', async () => {
    let ms = new MemoryStore();
    let a = [Kind.Value, Kind.List, Kind.Int8, false, [0, 1, 2]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();
    assert.deepEqual([0, 1, 2], v);
  });

  test('read compound list', async () => {
    let ms = new MemoryStore();

    let ltr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    let r1 = writeValue([0, 1], ltr, ms);
    let r2 = writeValue([2, 3], ltr, ms);
    let r3 = writeValue([4, 5], ltr, ms);
    let tuples = [
      new MetaTuple(r1, 2),
      new MetaTuple(r2, 4),
      new MetaTuple(r3, 6)
    ];
    let l = new CompoundList(ms, ltr, tuples);

    let a = [Kind.List, Kind.Int32, true, [r1.toString(), 2, r2.toString(), 4, r3.toString(), 6]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();
    invariant(v instanceof CompoundList);
    assert.isTrue(v.ref.equals(l.ref));
  });

  function assertMapsEqual(expected: Map, actual: Map): void {
    assert.strictEqual(expected.size, actual.size);
    expected.forEach((v, k) => {
      assert.isTrue(actual.has(k));
      assert.deepEqual(v, actual.get(k));
    });
  }

  test('read map of int64 to float64', async () => {
    let ms = new MemoryStore();
    let a = [Kind.Map, Kind.Int64, Kind.Float64, false, [0, 1, 2, 3]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    let m = new Map();
    m.set(0, 1);
    m.set(2, 3);

    assertMapsEqual(m, v);
  });

  test('read value map of uint64 to uint32', async () => {
    let ms = new MemoryStore();
    let a = [Kind.Value, Kind.Map, Kind.Uint64, Kind.Uint32, false, [0, 1, 2, 3]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    let m = new Map();
    m.set(0, 1);
    m.set(2, 3);

    assertMapsEqual(m, v);
  });

  function assertSetsEqual(expected: Set, actual: Set): void {
    assert.strictEqual(expected.size, actual.size);
    expected.forEach((v) => {
      assert.isTrue(actual.has(v));
    });
  }

  test('read set of uint8', async () => {
    let ms = new MemoryStore();
    let a = [Kind.Set, Kind.Uint8, false, [0, 1, 2, 3]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    let s = new Set();
    s.add(0);
    s.add(1);
    s.add(2);
    s.add(3);

    assertSetsEqual(s, v);
  });

  test('read value set of uint16', async () => {
    let ms = new MemoryStore();
    let a = [Kind.Value, Kind.Set, Kind.Uint16, false, [0, 1, 2, 3]];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    let s = new Set([0, 1, 2, 3]);
    assertSetsEqual(s, v);
  });

  function assertStruct(s: Struct, desc: TypeDesc, data: {[key: string]: any}) {
    invariant(s instanceof Struct);
    assert.deepEqual(desc, s.desc);

    for (let key in data) {
      assert.deepEqual(data[key], s.get(key));
    }
  }

  test('test read struct', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('s', makePrimitiveType(Kind.String), false),
      new Field('b', makePrimitiveType(Kind.Bool), false)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, 42, 'hi', true];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true
    });
  });

  test('test read struct union', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A2', [
      new Field('x', makePrimitiveType(Kind.Float32), false)
    ], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false)
    ]);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, 42, 1, 'hi'];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi'
    });
  });

  test('test read struct optional', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A3', [
      new Field('x', makePrimitiveType(Kind.Float32), false),
      new Field('s', makePrimitiveType(Kind.String), true),
      new Field('b', makePrimitiveType(Kind.Bool), true)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, 42, false, true, false];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      b: false
    });
  });

  test('test read struct with list', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A4', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('l', makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32)), false),
      new Field('s', makePrimitiveType(Kind.String), false)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, true, false, [0, 1, 2], 'hi'];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      l: [0, 1, 2],
      s: 'hi'
    });
  });

  test('test read struct with value', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A5', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('v', makePrimitiveType(Kind.Value), false),
      new Field('s', makePrimitiveType(Kind.String), false)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, true, Kind.Uint8, 42, 'hi'];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      b: true,
      v: 42,
      s: 'hi'
    });
  });

  test('test read value struct', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('s', makePrimitiveType(Kind.String), false),
      new Field('b', makePrimitiveType(Kind.Bool), false)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Value, Kind.Unresolved, pkg.ref.toString(), 0, 42, 'hi', true];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      s: 'hi',
      b: true
    });
  });

  test('test read enum', async () => {
    let ms = new MemoryStore();

    let tr = makeEnumType('E', ['a', 'b', 'c']);
    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, 1];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assert.deepEqual(1, v);
  });

  test('test read value enum', async () => {
    let ms = new MemoryStore();

    let tr = makeEnumType('E', ['a', 'b', 'c']);
    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Value, Kind.Unresolved, pkg.ref.toString(), 0, 1];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assert.deepEqual(1, v);
  });

  test('test read struct with', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('A1', [
      new Field('x', makePrimitiveType(Kind.Int16), false),
      new Field('e', makeType(new Ref(), 1), false),
      new Field('b', makePrimitiveType(Kind.Bool), false)
    ], []);
    let enumTref = makeEnumType('E', ['a', 'b', 'c']);
    let pkg = new Package([tr, enumTref], []);
    registerPackage(pkg);

    let a = [Kind.Unresolved, pkg.ref.toString(), 0, 42, 1, true];
    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    assertStruct(v, tr.desc, {
      x: 42,
      e: 1,
      b: true
    });
  });

  test('test read map of string to struct', async () => {
    let ms = new MemoryStore();
    let tr = makeStructType('s', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('i', makePrimitiveType(Kind.Int32), false)
    ], []);

    let pkg = new Package([tr], []);
    registerPackage(pkg);

    let a = [Kind.Value, Kind.Map, Kind.String, Kind.Unresolved, pkg.ref.toString(), 0, false, ['foo', true, 3, 'bar', false, 2, 'baz', false, 1]];

    let r = new JsonArrayReader(a, ms);
    let v = await r.readTopLevelValue();

    invariant(v instanceof Map);
    assert.strictEqual(3, v.size);

    assertStruct(v.get('foo'), tr.desc, {b: true, i: 3});
    assertStruct(v.get('bar'), tr.desc, {b: false, i: 2});
    assertStruct(v.get('baz'), tr.desc, {b: false, i: 1});
  });

  test('decodeNomsValue', async () => {
    let chunk = Chunk.fromString(`t [${Kind.Value}, ${Kind.Set}, ${Kind.Uint16}, false, [0, 1, 2, 3]]`);
    let v = await decodeNomsValue(chunk, new MemoryStore());
    let s = new Set([0, 1, 2, 3]);
    assertSetsEqual(s, v);
  });

  test('decodeNomsValue: counter with one commit', async () => {
    let ms = new MemoryStore();
    let root = Ref.parse('sha1-238be83f9eb4d346b06a82eb6bd0310b68189d24');
    ms.put(Chunk.fromString('t [15,11,16,21,"sha1-7546d804d845125bc42669c7a4c3f3fb909eca29",0,false,["counter","sha1-3d5f81a6640300f377d5c0257c7bdee094ff90de"]]')); // root
    ms.put(Chunk.fromString('t [22,[19,"Commit",["value",13,false,"parents",17,[16,[21,"sha1-0000000000000000000000000000000000000000",0]],false],[]],[]]')); // datas package
    ms.put(Chunk.fromString('t [21,"sha1-7546d804d845125bc42669c7a4c3f3fb909eca29",0,4,1,false,[]]')); // commit

    let rootMap = await readValue(root, ms);
    let counterRef = rootMap.get('counter');
    let commit = await readValue(counterRef, ms);
    assert.strictEqual(1, commit.get('value'));
  });

  test('top level blob', async () => {
    function stringToBuffer(s) {
      let bytes = new Uint8Array(s.length);
      for (let i = 0; i < s.length; i++) {
        bytes[i] = s.charCodeAt(i);
      }
      return bytes.buffer;
    }

    let chunk = Chunk.fromString('b hi');
    let v = await decodeNomsValue(chunk, new MemoryStore());
    assert.equal(2, v.byteLength);
    assert.deepEqual(stringToBuffer('hi'), v);

    let data = new Uint8Array(2 + 256);
    data[0] = 'b'.charCodeAt(0);
    data[1] = ' '.charCodeAt(0);
    let bytes = new Uint8Array(256);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
      data[2 + i] = i;
    }

    let chunk2 = new Chunk(data);
    let v2 = await decodeNomsValue(chunk2, new MemoryStore());
    assert.equal(bytes.buffer.byteLength, v2.byteLength);
    assert.deepEqual(bytes.buffer, v2);
  });
});
