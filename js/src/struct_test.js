// @flow

import Struct from './struct.js';
import {assert} from 'chai';
import {Field, makePrimitiveType, makeStructType, makeType} from './type.js';
import {Kind} from './noms_kind.js';
import {notNull} from './assert.js';
import {Package, registerPackage} from './package.js';
import {suite, test} from 'mocha';

suite('Struct', () => {
  test('equals', () => {
    let typeDef = makeStructType('S1', [
      new Field('x', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true)
    ], []);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let data1 = {x: true};
    let s1 = new Struct(type, typeDef, data1);
    let s2 = new Struct(type, typeDef, data1);

    assert.isTrue(s1.equals(s2));
  });

  // TODO: 'struct chunks', 'chunks optional', 'chunks union'

  test('new', () => {
    let typeDef = makeStructType('S2', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true)
    ], []);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.get('b'));
    assert.isFalse(s1.has('o'));
    assert.isFalse(s1.has('x'));

    let s2 = new Struct(type, typeDef, {b: false, o: 'hi'});
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
    let typeDef = makeStructType('S3', [], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), false)
    ]);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(true, s1.get('b'));
    assert.isFalse(s1.has('o'));
  });

  test('struct set', () => {
    let typeDef = makeStructType('S3', [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('o', makePrimitiveType(Kind.String), true)
    ], []);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let s1 = new Struct(type, typeDef, {b: true});
    let s2 = s1.set('b', false);

    // TODO: assert throws on set wrong type
    assert.throws(() => {
      s1.set('x', 1);
    });

    let s3 = s2.set('b', true);
    assert.isTrue(s1.equals(s3));
  });

  test('struct forEach', () => {
    let field1 = new Field('b', makePrimitiveType(Kind.Bool), false);
    let field2 = new Field('o', makePrimitiveType(Kind.String), false);
    let typeDef = makeStructType('S3', [field1, field2], []);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let s1 = new Struct(type, typeDef, {b: true, o: 'hi'});
    let expect = [true, 'b', field1, 'hi', 'o', field2];
    s1.forEach((v, k, f) => {
      assert.strictEqual(expect.shift(), v);
      assert.strictEqual(expect.shift(), k);
      assert.strictEqual(expect.shift(), f);
    });
  });

  test('struct set union', () => {
    let typeDef = makeStructType('S3', [], [
      new Field('b', makePrimitiveType(Kind.Bool), false),
      new Field('s', makePrimitiveType(Kind.String), false)
    ]);

    let pkg = new Package([typeDef], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;
    let type = makeType(pkgRef, 0);

    let s1 = new Struct(type, typeDef, {b: true});
    assert.strictEqual(0, s1.unionIndex);
    assert.strictEqual(true, s1.get(notNull(s1.unionField).name));
    assert.isFalse(s1.has('s'));

    let s2 = s1.set('s', 'hi');
    assert.strictEqual(1, s2.unionIndex);
    assert.strictEqual('hi', s2.get(notNull(s2.unionField).name));
    assert.isFalse(s2.has('b'));

    let s3 = s2.set('b', true);
    assert.isTrue(s1.equals(s3));
  });
});
