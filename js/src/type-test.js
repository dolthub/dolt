// @flow

import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {Field, makeCompoundType, makePrimitiveType, makeStructType, makeType} from './type.js';
import {Kind} from './noms-kind.js';
import {Package, registerPackage} from './package.js';
import {suite, test} from 'mocha';
import {DataStore} from './data-store.js';

suite('Type', () => {
  test('types', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const boolType = makePrimitiveType(Kind.Bool);
    const uint8Type = makePrimitiveType(Kind.Uint8);
    const stringType = makePrimitiveType(Kind.String);
    const mapType = makeCompoundType(Kind.Map, stringType, uint8Type);
    const setType = makeCompoundType(Kind.Set, stringType);
    const mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true),
    ], []);
    const otherType = makeStructType('MahOtherStruct', [], [
      new Field('StructField', mahType, false),
      new Field('StringField', stringType, false),
    ]);

    const pkgRef = Ref.parse('sha1-0123456789abcdef0123456789abcdef01234567');
    const trType = makeType(pkgRef, 42);

    const otherRef = ds.writeValue(otherType, otherType.type);
    const mapRef = ds.writeValue(mapType, mapType.type);
    const setRef = ds.writeValue(setType, setType.type);
    const mahRef = ds.writeValue(mahType, mahType.type);
    const trRef = ds.writeValue(trType, trType.type);

    assert.isTrue(otherType.equals(await ds.readValue(otherRef)));
    assert.isTrue(mapType.equals(await ds.readValue(mapRef)));
    assert.isTrue(setType.equals(await ds.readValue(setRef)));
    assert.isTrue(mahType.equals(await ds.readValue(mahRef)));
    assert.isTrue(trType.equals(await ds.readValue(trRef)));
  });

  test('typeRef describe', async () => {
    const boolType = makePrimitiveType(Kind.Bool);
    const uint8Type = makePrimitiveType(Kind.Uint8);
    const stringType = makePrimitiveType(Kind.String);
    const mapType = makeCompoundType(Kind.Map, stringType, uint8Type);
    const setType = makeCompoundType(Kind.Set, stringType);

    assert.strictEqual('Bool', boolType.describe());
    assert.strictEqual('Uint8', uint8Type.describe());
    assert.strictEqual('String', stringType.describe());
    assert.strictEqual('Map<String, Uint8>', mapType.describe());
    assert.strictEqual('Set<String>', setType.describe());

    const mahType = makeStructType('MahStruct',[
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true),
    ], [
    ]);
    assert.strictEqual('struct MahStruct {\n  Field1: String\n  Field2: optional Bool\n}',
        mahType.describe());

    const otherType = makeStructType('MahOtherStruct',[
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true),
    ], [
      new Field('Uint8Field', uint8Type, false),
      new Field('StringField', stringType, false),
    ]);

    const exp = `struct MahOtherStruct {\n  Field1: String\n  Field2: optional Bool\n  union {\n    Uint8Field: Uint8\n    StringField: String\n  }\n}`; // eslint-disable-line max-len
    assert.strictEqual(exp, otherType.describe());
  });

  test('type with pkgRef', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const pkg = new Package([makePrimitiveType(Kind.Float64)], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;

    const unresolvedType = makeType(pkgRef, 42);
    const unresolvedRef = ds.writeValue(unresolvedType, unresolvedType.type);

    const v = await ds.readValue(unresolvedRef);
    assert.isNotNull(v);
    assert.isTrue(pkgRef.equals(v.chunks[0]));
    const p = await ds.readValue(pkgRef);
    assert.isNotNull(p);
  });

  test('type Type', () => {
    assert.isTrue(makePrimitiveType(Kind.Bool).type.equals(makePrimitiveType(Kind.Type)));
  });

  test('empty package ref', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const v = makeType(new Ref(), -1);
    const r = ds.writeValue(v, v.type);
    const v2 = await ds.readValue(r);
    assert.isTrue(v.equals(v2));
  });
});
