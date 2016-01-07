// @flow

import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {Field, makeCompoundType, makePrimitiveType, makeStructType, makeType} from './type.js';
import {Kind} from './noms_kind.js';
import {Package, registerPackage} from './package.js';
import {readValue} from './read_value.js';
import {suite, test} from 'mocha';
import {writeValue} from './encode.js';

suite('Type', () => {
  test('types', async () => {
    const ms = new MemoryStore();

    const boolType = makePrimitiveType(Kind.Bool);
    const uint8Type = makePrimitiveType(Kind.Uint8);
    const stringType = makePrimitiveType(Kind.String);
    const mapType = makeCompoundType(Kind.Map, stringType, uint8Type);
    const setType = makeCompoundType(Kind.Set, stringType);
    const mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true)
    ], []);
    const otherType = makeStructType('MahOtherStruct', [], [
      new Field('StructField', mahType, false),
      new Field('StringField', stringType, false)
    ]);

    const pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const trType = makeType(pkgRef, 42);

    const otherRef = writeValue(otherType, otherType.type, ms);
    const mapRef = writeValue(mapType, mapType.type, ms);
    const setRef = writeValue(setType, setType.type, ms);
    const mahRef = writeValue(mahType, mahType.type, ms);
    const trRef = writeValue(trType, trType.type, ms);

    assert.isTrue(otherType.equals(await readValue(otherRef, ms)));
    assert.isTrue(mapType.equals(await readValue(mapRef, ms)));
    assert.isTrue(setType.equals(await readValue(setRef, ms)));
    assert.isTrue(mahType.equals(await readValue(mahRef, ms)));
    assert.isTrue(trType.equals(await readValue(trRef, ms)));
  });

  test('type with pkgRef', async () => {
    const ms = new MemoryStore();

    const pkg = new Package([makePrimitiveType(Kind.Float64)], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;

    const unresolvedType = makeType(pkgRef, 42);
    const unresolvedRef = writeValue(unresolvedType, unresolvedType.type, ms);

    const v = await readValue(unresolvedRef, ms);
    assert.isNotNull(v);
    assert.isTrue(pkgRef.equals(v.chunks[0]));
    const p = await readValue(pkgRef, ms);
    assert.isNotNull(p);
  });

  test('type Type', () => {
    assert.isTrue(makePrimitiveType(Kind.Bool).type.equals(makePrimitiveType(Kind.Type)));
  });

  test('empty package ref', async () => {
    const ms = new MemoryStore();
    const v = makeType(new Ref(), -1);
    const r = writeValue(v, v.type, ms);
    const v2 = await readValue(r, ms);
    assert.isTrue(v.equals(v2));
  });
});
