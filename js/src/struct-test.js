// @flow

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {default as Struct, newStruct, StructMirror, createStructClass} from './struct.js';
import {assert} from 'chai';
import {
  boolType,
  numberType,
  makeStructType,
  makeRefType,
  makeListType,
  stringType,
  valueType,
} from './type.js';
import {suite, test} from 'mocha';
import Database from './database.js';
import {newList} from './list.js';
import {equals} from './compare.js';

suite('Struct', () => {
  test('equals', () => {
    const type = makeStructType('S1', {
      'x': boolType,
      'o': stringType,
    });

    const data1 = {x: true, o: 'hi'};
    const s1 = newStruct(type, data1);
    const s2 = newStruct(type, data1);

    assert.isTrue(equals(s1, s2));
  });

  test('chunks', () => {
    const ds = new Database(makeTestingBatchStore());

    const bt = boolType;
    const refOfBoolType = makeRefType(bt);
    const type = makeStructType('S1', {
      'r': refOfBoolType,
    });

    const b = true;
    const r = ds.writeValue(b);
    const s1 = newStruct(type, {r: r});
    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(equals(r, s1.chunks[0]));
  });

  test('new', () => {
    const type = makeStructType('S2', {
      'b': boolType,
      'o': stringType,
    });

    const s1 = newStruct(type, {b: true, o: 'hi'});
    assert.strictEqual(s1.b, true);
    assert.strictEqual(s1.o, 'hi');

    const s2 = newStruct(type, {b: false, o: 'hi'});
    assert.strictEqual(s2.b, false);
    assert.strictEqual(s2.o, 'hi');

    assert.throws(() => {
      newStruct(type, {o: 'hi'}); // missing required field
    });

    assert.throws(() => {
      newStruct(type, {x: 'hi'}); // unknown field
    });

    const s3 = newStruct(type, {b: true, o: 'hi'});
    assert.isTrue(equals(s1, s3));
  });

  test('struct set', () => {
    const type = makeStructType('S3', {
      'b': boolType,
      'o': stringType,
    });

    const s1 = newStruct(type, {b: true, o: 'hi'});
    const s2 = s1.setB(false);

    // TODO: assert throws on set wrong type
    assert.throws(() => {
      s1.setX(1);
    });

    const s3 = s2.setB(true);
    assert.isTrue(equals(s1, s3));

    const m = new StructMirror(s1);
    const s4 = m.set('b', false);
    assert.isTrue(equals(s2, s4));

    const s5 = s3.setO('bye');
    const s6 = new StructMirror(s3).set('o', 'bye');
    assert.isTrue(equals(s5, s6));
  });

  test('type assertion on construct', () => {
    assert.throws(() => {
      newStruct(boolType, {b: true});
    });
  });

  test('createStructClass', () => {
    const typeA = makeStructType('A', {
      'b': numberType,
      'c': stringType,
    });
    const A = createStructClass(typeA);
    const a = new A({b: 1, c: 'hi'});
    assert.instanceOf(a, Struct);
    assert.instanceOf(a, A);
    assert.equal(a.b, 1);
    assert.equal(a.c, 'hi');
  });

  test('type validation', () => {
    const type = makeStructType('S1', {
      'x': boolType,
      'o': stringType,
    });

    assert.throws(() => {
      newStruct(type, {x: 1, o: 'hi'});
    });
    assert.throws(() => {
      newStruct(type, {o: 1});
    });

    newStruct(type, {x: true, o: 'hi'});
  });

  test('type validation cyclic', async () => {
    // struct S {
    //   b: Bool
    //   l: List<S>
    // }
    const type = makeStructType('S', {
      'b': boolType,
      'l': valueType, // placeholder
    });
    const listType = makeListType(type);
    type.desc.fields['l'] = listType;

    const emptyList = await newList([], listType);
    newStruct(type, {b: true, l: emptyList});
    newStruct(type, {b: true, l: await newList([
      newStruct(type, {b: false, l: emptyList}),
    ], listType)});

    assert.throws(() => {
      newStruct(type, {b: 1});
    });
    assert.throws(() => {
      newStruct(type, {b: true, o: 1});
    });
  });
});
