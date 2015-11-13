/* @flow */

'use strict';

import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {Field, makeCompoundType, makePrimitiveType, makeStructType, makeType} from './type.js';
import {Kind} from './noms_kind.js';
import {Package, registerPackage} from './package.js';
import {readValue} from './decode.js';
import {suite, test} from 'mocha';
import {writeValue} from './encode.js';

suite('Type', () => {
  test('types', async () => {
    let ms = new MemoryStore();

    let boolType = makePrimitiveType(Kind.Bool);
    let uint8Type = makePrimitiveType(Kind.UInt8);
    let stringType = makePrimitiveType(Kind.String);
    let mapType = makeCompoundType(Kind.Map, stringType, uint8Type);
    let setType = makeCompoundType(Kind.Set, stringType);
    let mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true)
    ], []);
    let otherType = makeStructType('MahOtherStruct', [], [
      new Field('StructField', mahType, false),
      new Field('StringField', stringType, false)
    ]);

    let pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    let trType = makeType(pkgRef, 42);

    let otherRef = writeValue(otherType, otherType.type, ms);
    let mapRef = writeValue(mapType, mapType.type, ms);
    let setRef = writeValue(setType, setType.type, ms);
    let mahRef = writeValue(mahType, mahType.type, ms);
    let trRef = writeValue(trType, trType.type, ms);

    assert.isTrue(otherType.equals(await readValue(otherRef, ms)));
    assert.isTrue(mapType.equals(await readValue(mapRef, ms)));
    assert.isTrue(setType.equals(await readValue(setRef, ms)));
    assert.isTrue(mahType.equals(await readValue(mahRef, ms)));
    assert.isTrue(trType.equals(await readValue(trRef, ms)));
  });

  test('type with pkgRef', async () => {
    let ms = new MemoryStore();

    let pkg = new Package([makePrimitiveType(Kind.Float64)], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;

    let unresolvedType = makeType(pkgRef, 42);
    let unresolvedRef = writeValue(unresolvedType, unresolvedType.type, ms);

    let v = await readValue(unresolvedRef, ms);
    assert.isNotNull(v);
    let p = await readValue(pkgRef, ms);
    assert.isNotNull(p);
  });

  test('type Type', () => {
    assert.isTrue(makePrimitiveType(Kind.Bool).type.equals(makePrimitiveType(Kind.Type)));
  });
});
