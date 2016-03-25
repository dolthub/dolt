// @flow

import MemoryStore from './memory-store.js';
import RefValue from './ref-value.js';
import Struct from './struct.js';
import {assert} from 'chai';
import {Field, makeCompoundType, makePrimitiveType, makeStructType, makeType} from './type.js';
import {Kind} from './noms-kind.js';
import {Package, registerPackage} from './package.js';
import {suite, test} from 'mocha';
import DataStore from './data-store.js';

suite('Struct', () => {
  test('equals', () => {
    const typeDef = makeStructType('S1', [
      new Field('x', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const data1 = {x: true};
    const s1 = new Struct(type, typeDef, data1);
    const s2 = new Struct(type, typeDef, data1);

    assert.isTrue(s1.equals(s2));
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = makePrimitiveType(Kind.Bool);
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [
      new Field('r', refOfBoolType, false),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const b = true;
    const r = new RefValue(ds.writeValue(b), refOfBoolType);
    const s1 = new Struct(type, typeDef, {r: r});
    assert.strictEqual(2, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));
    assert.isTrue(r.equals(s1.chunks[1]));
  });

  test('chunks optional', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = makePrimitiveType(Kind.Bool);
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [
      new Field('r', refOfBoolType, true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {});

    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));

    const b = true;
    const r = new RefValue(ds.writeValue(b), refOfBoolType);
    const s2 = new Struct(type, typeDef, {r: r});
    assert.strictEqual(2, s2.chunks.length);
    assert.isTrue(pkgRef.equals(s2.chunks[0].targetRef));
    assert.isTrue(r.equals(s2.chunks[1]));
  });

  test('chunks union', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const bt = makePrimitiveType(Kind.Bool);
    const refOfBoolType = makeCompoundType(Kind.Ref, bt);
    const typeDef = makeStructType('S1', [], [
      new Field('r', refOfBoolType, false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {s: 'hi'});
    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(pkgRef.equals(s1.chunks[0].targetRef));

    const b = true;
    const r = new RefValue(ds.writeValue(b), refOfBoolType);
    const s2 = new Struct(type, typeDef, {r: r});
    assert.strictEqual(2, s2.chunks.length);
    assert.isTrue(pkgRef.equals(s2.chunks[0].targetRef));
    assert.isTrue(r.equals(s2.chunks[1]));
  });

  test('new', () => {
    const typeDef = makeStructType('S2', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.get('b'));
    assert.isFalse(s1.has('o'));
    assert.isFalse(s1.has('x'));

    const s2 = new Struct(type, typeDef, {b: false, o: 'hi'});
    assert.strictEqual(false, s2.get('b'));
    assert.isTrue(s2.has('o'));
    assert.strictEqual('hi', s2.get('o'));

    assert.throws(() => {
      new Struct(type, typeDef, {o: 'hi'}); // missing required field
    });

    assert.throws(() => {
      new Struct(type, typeDef, {x: 'hi'}); // unknown field
    });
  });

  test('new union', () => {
    const typeDef = makeStructType('S3', [], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.get('b'));
    assert.isFalse(s1.has('o'));
  });

  test('struct set', () => {
    const typeDef = makeStructType('S3', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {b: true});
    const s2 = s1.set('b', false);

    // TODO: assert throws on set wrong type
    assert.throws(() => {
      s1.set('x', 1);
    });

    const s3 = s2.set('b', true);
    assert.isTrue(s1.equals(s3));
  });

  test('struct set union', () => {
    const typeDef = makeStructType('S3', [], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false),
    ]);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(0, s1.unionIndex);
    assert.strictEqual(true, s1.unionValue);
    assert.isFalse(s1.has('s'));

    const s2 = s1.set('s', 'hi');
    assert.strictEqual(1, s2.unionIndex);
    assert.strictEqual('hi', s2.unionValue);
    assert.isFalse(s2.has('b'));

    const s3 = s2.set('b', true);
    assert.isTrue(s1.equals(s3));
  });

  test('type assertion on construct', () => {
    const typeDef = makeStructType('S3', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
    ], []);

    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    assert.throws(() => {
      new Struct(type, type, {b: true});
    });

    assert.throws(() => {
      new Struct(typeDef, typeDef, {b: true});
    });
  });
});
