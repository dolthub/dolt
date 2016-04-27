// @flow

import MemoryStore from './memory-store.js';
import {default as Ref, emptyRef} from './ref.js';
import {assert} from 'chai';
import {
  boolType,
  Field,
  makeCompoundType,
  makeStructType,
  makeType,
  numberType,
  stringType,
  typeType,
} from './type.js';
import {Kind} from './noms-kind.js';
import {Package, registerPackage} from './package.js';
import {suite, test} from 'mocha';
import DataStore from './data-store.js';

suite('Type', () => {
  test('types', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const mapType = makeCompoundType(Kind.Map, stringType, numberType);
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

    const otherRef = ds.writeValue(otherType).targetRef;
    const mapRef = ds.writeValue(mapType).targetRef;
    const setRef = ds.writeValue(setType).targetRef;
    const mahRef = ds.writeValue(mahType).targetRef;
    const trRef = ds.writeValue(trType).targetRef;

    assert.isTrue(otherType.equals(await ds.readValue(otherRef)));
    assert.isTrue(mapType.equals(await ds.readValue(mapRef)));
    assert.isTrue(setType.equals(await ds.readValue(setRef)));
    assert.isTrue(mahType.equals(await ds.readValue(mahRef)));
    assert.isTrue(trType.equals(await ds.readValue(trRef)));
  });

  test('typeRef describe', async () => {
    const mapType = makeCompoundType(Kind.Map, stringType, numberType);
    const setType = makeCompoundType(Kind.Set, stringType);

    assert.strictEqual('Bool', boolType.describe());
    assert.strictEqual('Number', numberType.describe());
    assert.strictEqual('String', stringType.describe());
    assert.strictEqual('Map<String, Number>', mapType.describe());
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
      new Field('NumberField', numberType, false),
      new Field('StringField', stringType, false),
    ]);

    const exp = `struct MahOtherStruct {\n  Field1: String\n  Field2: optional Bool\n  union {\n    NumberField: Number\n    StringField: String\n  }\n}`; // eslint-disable-line max-len
    assert.strictEqual(exp, otherType.describe());
  });

  test('type with pkgRef', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const pkg = new Package([numberType], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;

    const unresolvedType = makeType(pkgRef, 42);
    const unresolvedRef = ds.writeValue(unresolvedType).targetRef;

    const v = await ds.readValue(unresolvedRef);
    assert.isNotNull(v);
    assert.isTrue(pkgRef.equals(v.chunks[0].targetRef));
    const p = await ds.readValue(pkgRef);
    assert.isNotNull(p);
  });

  test('type Type', () => {
    assert.isTrue(boolType.type.equals(typeType));
  });

  test('empty package ref', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const v = makeType(emptyRef, -1);
    const r = ds.writeValue(v).targetRef;
    const v2 = await ds.readValue(r);
    assert.isTrue(v.equals(v2));
  });
});
