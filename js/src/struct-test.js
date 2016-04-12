// @flow

import MemoryStore from './memory-store.js';
import {newStruct, StructMirror, createStructClass} from './struct.js';
import {assert} from 'chai';
import {
  boolType,
  Field,
  float64Type,
  makeCompoundType,
  makeStructType,
  makeType,
  stringType,
} from './type.js';
import {Kind} from './noms-kind.js';
import {Package, registerPackage} from './package.js';
import {suite, test} from 'mocha';
import DataStore from './data-store.js';
import Ref from './ref.js';

suite('Struct', () => {
  test('equals', () => {
    const typeDef = makeStructType('S1', [
      new Field('x', boolType, false),
      new Field('o', stringType, true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const data1 = {x: true};
    const s1 = newStruct(type, typeDef, data1);
    const s2 = newStruct(type, typeDef, data1);

    assert.isTrue(s1.equals(s2));
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = boolType;
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [
      new Field('r', refOfBoolType, false),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const b = true;
    const r = ds.writeValue(b);
    const s1 = newStruct(type, typeDef, {r: r});
    assert.strictEqual(2, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));
    assert.isTrue(r.equals(s1.chunks[1]));
  });

  test('chunks optional', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = boolType;
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [
      new Field('r', refOfBoolType, true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {});

    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));

    const b = true;
    const r = ds.writeValue(b);
    const s2 = newStruct(type, typeDef, {r: r});
    assert.strictEqual(2, s2.chunks.length);
    assert.isTrue(pkgRef.equals(s2.chunks[0].targetRef));
    assert.isTrue(r.equals(s2.chunks[1]));
  });

  test('chunks union', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = boolType;
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [], [
      new Field('r', refOfBoolType, false),
      new Field('s', stringType, false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {s: 'hi'});
    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));

    const b = true;
    const r = ds.writeValue(b);
    const s2 = newStruct(type, typeDef, {r: r});
    assert.strictEqual(2, s2.chunks.length);
    assert.isTrue(pkgRef.equals(s2.chunks[0].targetRef));
    assert.isTrue(r.equals(s2.chunks[1]));
  });

  test('new', () => {
    const typeDef = makeStructType('S2', [
      new Field('b', boolType, false),
      new Field('o', stringType, true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.b);
    assert.strictEqual(s1.o, undefined);

    const s2 = newStruct(type, typeDef, {b: false, o: 'hi'});
    assert.strictEqual(false, s2.b);
    assert.strictEqual('hi', s2.o);

    assert.throws(() => {
      newStruct(type, typeDef, {o: 'hi'}); // missing required field
    });

    assert.throws(() => {
      newStruct(type, typeDef, {x: 'hi'}); // unknown field
    });

    const s3 = newStruct(type, typeDef, {b: true, o: undefined});
    assert.isTrue(s1.equals(s3));
  });

  test('new union', () => {
    const typeDef = makeStructType('S3', [], [
      new Field('b', boolType, false),
      new Field('o', stringType, false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.b);
    assert.strictEqual(s1.o, undefined);
  });

  test('struct set', () => {
    const typeDef = makeStructType('S3', [
      new Field('b', boolType, false),
      new Field('o', stringType, true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {b: true});
    const s2 = s1.setB(false);

    // TODO: assert throws on set wrong type
    assert.throws(() => {
      s1.setX(1);
    });

    const s3 = s2.setB(true);
    assert.isTrue(s1.equals(s3));

    const m = new StructMirror(s1);
    const s4 = m.set('b', false);
    assert.isTrue(s2.equals(s4));

    const s5 = s3.setO(undefined);
    const s6 = new StructMirror(s3).set('o', undefined);
    assert.isTrue(s5.equals(s6));
  });

  test('struct set union', () => {
    const typeDef = makeStructType('S3', [], [
      new Field('b', boolType, false),
      new Field('s', stringType, false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = newStruct(type, typeDef, {b: true});
    const m1 = new StructMirror(s1);
    assert.strictEqual(0, m1.unionIndex);
    assert.strictEqual(true, m1.unionValue);
    assert.strictEqual(s1.s, undefined);

    const s2 = s1.setS('hi');
    const m2 = new StructMirror(s2);
    assert.strictEqual(1, m2.unionIndex);
    assert.strictEqual('hi', m2.unionValue);
    assert.strictEqual(s2.b, undefined);
    assert.isFalse(m2.has('b'));

    const s3 = s2.setB(true);
    assert.isTrue(s1.equals(s3));
  });

  test('type assertion on construct', () => {
    const typeDef = makeStructType('S3', [
      new Field('b', boolType, false),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    assert.throws(() => {
      newStruct(type, type, {b: true});
    });

    assert.throws(() => {
      newStruct(typeDef, typeDef, {b: true});
    });
  });

  test('named union', () => {
    let typeDef, typeDefA, typeDefD;
    const pkg = new Package([
      typeDef = makeStructType('StructWithUnions', [
        new Field('a', makeType(new Ref(), 1), false),
        new Field('d', makeType(new Ref(), 2), false),
      ], []),
      typeDefA = makeStructType('', [], [
        new Field('b', float64Type, false),
        new Field('c', stringType, false),
      ]),
      typeDefD = makeStructType('', [], [
        new Field('e', float64Type, false),
        new Field('f', stringType, false),
      ]),
    ], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);
    const typeA = makeType(pkgRef, 1);
    const typeD = makeType(pkgRef, 2);

    const StructWithUnions = createStructClass(type, typeDef);
    const A = createStructClass(typeA, typeDefA);
    const D = createStructClass(typeD, typeDefD);

    const s = new StructWithUnions({
      a: new A({b: 1}),
      d: new D({e: 2}),
    });

    assert.equal(s.a.b, 1);
    assert.equal(s.d.e, 2);

    const s2 = s.setA(s.a.setC('hi'));
    assert.equal(s2.a.c, 'hi');
    assert.equal(s2.a.b, undefined);

    const s3 = s2.setD(s.d.setF('bye'));
    assert.equal(s3.d.f, 'bye');
    assert.equal(s3.d.e, undefined);

    assert.isTrue(s3.equals(new StructWithUnions({
      a: new A({c: 'hi'}),
      d: new D({f: 'bye'}),
    })));
  });
});
