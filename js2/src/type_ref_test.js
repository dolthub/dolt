/* @flow */

'use strict';

import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {Field, makeCompoundTypeRef, makePrimitiveTypeRef, makeStructTypeRef, makeTypeRef} from './type_ref.js';
import {Kind} from './noms_kind.js';
import {Package, registerPackage} from './package.js';
import {readValue} from './decode.js';
import {suite, test} from 'mocha';
import {writeValue} from './encode.js';

suite('Type Ref', () => {
  test('types', async () => {
    let ms = new MemoryStore();

    let boolType = makePrimitiveTypeRef(Kind.Bool);
    let uint8Type = makePrimitiveTypeRef(Kind.UInt8);
    let stringType = makePrimitiveTypeRef(Kind.String);
    let mapType = makeCompoundTypeRef(Kind.Map, stringType, uint8Type);
    let setType = makeCompoundTypeRef(Kind.Set, stringType);
    let mahType = makeStructTypeRef('MahStruct', [
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true)
    ], []);
    let otherType = makeStructTypeRef('MahOtherStruct', [], [
      new Field('StructField', mahType, false),
      new Field('StringField', stringType, false)
    ]);

    let pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    let trType = makeTypeRef(pkgRef, 42);

    let otherRef = writeValue(otherType, otherType.typeRef, ms);
    let mapRef = writeValue(mapType, mapType.typeRef, ms);
    let setRef = writeValue(setType, setType.typeRef, ms);
    let mahRef = writeValue(mahType, mahType.typeRef, ms);
    let trRef = writeValue(trType, trType.typeRef, ms);

    assert.isTrue(otherType.equals(await readValue(otherRef, ms)));
    assert.isTrue(mapType.equals(await readValue(mapRef, ms)));
    assert.isTrue(setType.equals(await readValue(setRef, ms)));
    assert.isTrue(mahType.equals(await readValue(mahRef, ms)));
    assert.isTrue(trType.equals(await readValue(trRef, ms)));
  });

  test('type with pkgRef', async () => {
    let ms = new MemoryStore();

    let pkg = new Package([makePrimitiveTypeRef(Kind.Float64)], []);
    registerPackage(pkg);
    let pkgRef = pkg.ref;

    let unresolvedType = makeTypeRef(pkgRef, 42);
    let unresolvedRef = writeValue(unresolvedType, unresolvedType.typeRef, ms);

    let v = await readValue(unresolvedRef, ms);
    assert.isNotNull(v);
    let p = await readValue(pkgRef, ms);
    assert.isNotNull(p);
  });

  test('typeRefTypeRef', () => {
    assert.isTrue(makePrimitiveTypeRef(Kind.Bool).typeRef.equals(makePrimitiveTypeRef(Kind.TypeRef)));
  });
});
