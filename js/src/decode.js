// @flow

import {NomsBlob, BlobLeafSequence} from './blob.js';
import Chunk from './chunk.js';
import Ref from './ref.js';
import RefValue from './ref-value.js';
import Struct from './struct.js';
import type DataStore from './data-store.js';
import type {NomsKind} from './noms-kind.js';
import {decode as decodeBase64} from './base64.js';
import {
  CompoundDesc,
  EnumDesc,
  Field,
  makeCompoundType,
  makeEnumType,
  makePrimitiveType,
  makeStructType,
  makeType,
  makeUnresolvedType,
  PrimitiveDesc,
  StructDesc,
  Type,
  UnresolvedDesc,
} from './type.js';
import {indexTypeForMetaSequence, MetaTuple, newMetaSequenceFromData} from './meta-sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {lookupPackage, Package, readPackage} from './package.js';
import {NomsMap, MapLeafSequence} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {IndexedMetaSequence} from './meta-sequence.js';

const typedTag = 't ';
const blobTag = 'b ';

class UnresolvedPackage {
  pkgRef: Ref;

  constructor(pkgRef: Ref) {
    this.pkgRef = pkgRef;
  }
}

export class JsonArrayReader {
  _a: Array<any>;
  _i: number;
  _ds: DataStore;

  constructor(a: Array<any>, ds: DataStore) {
    this._a = a;
    this._i = 0;
    this._ds = ds;
  }

  read(): any {
    return this._a[this._i++];
  }

  atEnd(): boolean {
    return this._i >= this._a.length;
  }

  readString(): string {
    const next = this.read();
    invariant(typeof next === 'string');
    return next;
  }

  readBool(): boolean {
    const next = this.read();
    invariant(typeof next === 'boolean');
    return next;
  }

  readInt(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseInt(next, 10);
  }

  readUint(): number {
    const v = this.readInt();
    invariant(v >= 0);
    return v;
  }

  readFloat(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseFloat(next);
  }

  readOrdinal(): number {
    return this.readInt();
  }

  readArray(): Array<any> {
    const next = this.read();
    invariant(Array.isArray(next));
    return next;
  }

  readKind(): NomsKind {
    const next = this.read();
    invariant(typeof next === 'number');
    return next;
  }

  readRef(): Ref {
    const next = this.readString();
    return Ref.parse(next);
  }

  readTypeAsTag(): Type {
    const kind = this.readKind();
    switch (kind) {
      case Kind.List:
      case Kind.Set:
      case Kind.Ref: {
        const elemType = this.readTypeAsTag();
        return makeCompoundType(kind, elemType);
      }
      case Kind.Map: {
        const keyType = this.readTypeAsTag();
        const valueType = this.readTypeAsTag();
        return makeCompoundType(kind, keyType, valueType);
      }
      case Kind.Type:
        return makePrimitiveType(Kind.Type);
      case Kind.Unresolved: {
        const pkgRef = this.readRef();
        const ordinal = this.readOrdinal();
        return makeType(pkgRef, ordinal);
      }
    }

    if (isPrimitiveKind(kind)) {
      return makePrimitiveType(kind);
    }

    throw new Error('Unreachable');
  }


  readBlobLeafSequence(): BlobLeafSequence {
    const bytes = decodeBase64(this.readString());
    return new BlobLeafSequence(this._ds, bytes);
  }

  readSequence(t: Type, pkg: ?Package): Array<any> {
    const elemType = t.elemTypes[0];
    const list = [];
    while (!this.atEnd()) {
      const v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type, pkg: ?Package): ListLeafSequence {
    const seq = this.readSequence(t, pkg);
    t = fixupType(t, pkg);
    return new ListLeafSequence(this._ds, t, seq);
  }

  readSetLeafSequence(t: Type, pkg: ?Package): SetLeafSequence {
    const seq = this.readSequence(t, pkg);
    t = fixupType(t, pkg);
    return new SetLeafSequence(this._ds, t, seq);
  }

  readMapLeafSequence(t: Type, pkg: ?Package): MapLeafSequence {
    const keyType = t.elemTypes[0];
    const valueType = t.elemTypes[1];
    const entries = [];
    while (!this.atEnd()) {
      const k = this.readValueWithoutTag(keyType, pkg);
      const v = this.readValueWithoutTag(valueType, pkg);
      entries.push({key: k, value: v});
    }

    t = fixupType(t, pkg);
    return new MapLeafSequence(this._ds, t, entries);
  }

  readEnum(): number {
    return this.readUint();
  }

  readMetaSequence(t: Type, pkg: ?Package): any {
    const data: Array<MetaTuple> = [];
    const indexType = indexTypeForMetaSequence(t);
    while (!this.atEnd()) {
      const ref = this.readRef();
      const v = this.readValueWithoutTag(indexType, pkg);
      data.push(new MetaTuple(ref, v));
    }

    t = fixupType(t, pkg);
    return newMetaSequenceFromData(this._ds, t, data);
  }

  readPackage(t: Type, pkg: ?Package): Package {
    const r2 = new JsonArrayReader(this.readArray(), this._ds);
    const types = [];
    while (!r2.atEnd()) {
      types.push(r2.readTypeAsValue(pkg));
    }

    const r3 = new JsonArrayReader(this.readArray(), this._ds);
    const deps = [];
    while (!r3.atEnd()) {
      deps.push(r3.readRef());
    }

    return new Package(types, deps);
  }

  readRefValue(t: Type, pkg: ?Package): RefValue {
    const ref = this.readRef();
    t = fixupType(t, pkg);
    return new RefValue(ref, t);
  }

  readTopLevelValue(): Promise<any> {
    return new Promise((resolve, reject) => {
      const t = this.readTypeAsTag();
      const doRead = () => {
        const i = this._i;

        try {
          const v = this.readValueWithoutTag(t);
          resolve(v);
        } catch (ex) {
          if (ex instanceof UnresolvedPackage) {
            readPackage(ex.pkgRef, this._ds).then(() => {
              this._i = i;
              doRead();
            });
          } else {
            reject(ex);
          }
        }
      };

      doRead();
    });
  }

  readValueWithoutTag(t: Type, pkg: ?Package = null): any {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this.readBool();
        let sequence;
        if (isMeta) {
          const r2 = new JsonArrayReader(this.readArray(), this._ds);
          sequence = r2.readMetaSequence(t, pkg);
          invariant(sequence instanceof IndexedMetaSequence);
        } else {
          sequence = this.readBlobLeafSequence();
        }
        return new NomsBlob(sequence);
      }
      case Kind.Bool:
        return this.readBool();
      case Kind.Float32:
      case Kind.Float64:
        return this.readFloat();
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
        return this.readInt();
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
        return this.readUint();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        const t2 = this.readTypeAsTag();
        return this.readValueWithoutTag(t2, pkg);
      }
      case Kind.List: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
            r2.readMetaSequence(t, pkg) :
            r2.readListLeafSequence(t, pkg);
        return new NomsList(t, sequence);
      }
      case Kind.Map: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readMapLeafSequence(t, pkg);
        return new NomsMap(t, sequence);
      }
      case Kind.Package:
        return this.readPackage(t, pkg);
      case Kind.Ref:
        return this.readRefValue(t, pkg);
      case Kind.Set: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readSetLeafSequence(t, pkg);
        return new NomsSet(t, sequence);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.Type:
        return this.readTypeAsValue(pkg);
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  readUnresolvedKindToValue(t: Type, pkg: ?Package = null): any {
    const pkgRef = t.packageRef;
    const ordinal = t.ordinal;
    if (!pkgRef.isEmpty()) {
      pkg = lookupPackage(pkgRef);
      if (!pkg) {
        throw new UnresolvedPackage(pkgRef);
      }
      invariant(pkg);
    }

    pkg = notNull(pkg);
    const typeDef = pkg.types[ordinal];
    if (typeDef.kind === Kind.Enum) {
      return this.readEnum();
    }

    invariant(typeDef.kind === Kind.Struct);
    return this.readStruct(typeDef, t, pkg);
  }

  readTypeAsValue(pkg: ?Package): Type {
    const k = this.readKind();

    switch (k) {
      case Kind.Enum:
        const name = this.readString();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const ids = [];
        while (!r2.atEnd()) {
          ids.push(r2.readString());
        }
        return makeEnumType(name, ids);
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const elemTypes: Array<Type> = [];
        while (!r2.atEnd()) {
          elemTypes.push(r2.readTypeAsValue());
        }

        return makeCompoundType(k, ...elemTypes);
      }
      case Kind.Struct: {
        const name = this.readString();
        const readFields = () => {
          const fields: Array<Field> = [];
          const fieldReader = new JsonArrayReader(this.readArray(), this._ds);
          while (!fieldReader.atEnd()) {
            const fieldName = fieldReader.readString();
            const fieldType = fieldReader.readTypeAsValue(pkg);
            const optional = fieldReader.readBool();
            fields.push(new Field(fieldName, fieldType, optional));
          }
          return fields;
        };

        const fields = readFields();
        const choices = readFields();
        return makeStructType(name, fields, choices);
      }
      case Kind.Unresolved: {
        const pkgRef = this.readRef();
        const ordinal = this.readOrdinal();
        if (ordinal === -1) {
          const namespace = this.readString();
          const name = this.readString();
          return makeUnresolvedType(namespace, name);
        }

        return makeType(pkgRef, ordinal);
      }
    }

    invariant(isPrimitiveKind(k));
    return makePrimitiveType(k);

  }

  readStruct(typeDef: Type, type: Type, pkg: Package): Struct {
    // TODO FixupType?
    const desc = typeDef.desc;
    invariant(desc instanceof StructDesc);

    const s: { [key: string]: any } = Object.create(null);

    for (let i = 0; i < desc.fields.length; i++) {
      const field = desc.fields[i];
      if (field.optional) {
        const b = this.readBool();
        if (b) {
          const v = this.readValueWithoutTag(field.t, pkg);
          s[field.name] = v;
        }
      } else {
        const v = this.readValueWithoutTag(field.t, pkg);
        s[field.name] = v;
      }
    }

    let unionIndex = -1;
    if (desc.union.length > 0) {
      unionIndex = this.readUint();
      const unionField = desc.union[unionIndex];
      const v = this.readValueWithoutTag(unionField.t, pkg);
      s[unionField.name] = v;
    }

    type = fixupType(type, pkg);
    return new Struct(type, typeDef, s);
  }
}

function fixupType(t: Type, pkg: ?Package): Type {
  return fixupTypeInternal(t, pkg) || t;
}

function fixupTypeInternal(t: Type, pkg: ?Package): ?Type {
  const desc = t.desc;
  if (desc instanceof EnumDesc || desc instanceof StructDesc) {
    throw new Error('not reached');
  }
  if (desc instanceof PrimitiveDesc) {
    return null;
  }
  if (desc instanceof CompoundDesc) {
    let changed = false;
    const newTypes = desc.elemTypes.map(t => {
      const newT = fixupTypeInternal(t, pkg);
      if (newT) {
        changed = true;
      }
      return newT || t;
    });

    return changed ? makeCompoundType(t.kind, ...newTypes) : null;
  }
  if (desc instanceof UnresolvedDesc) {
    if (t.hasPackageRef) {
      return null;
    }

    return makeType(notNull(pkg).ref, t.ordinal);
  }
}

export function decodeNomsValue(chunk: Chunk, ds: DataStore): Promise<any> {
  const tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      const payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      const reader = new JsonArrayReader(payload, ds);
      return reader.readTopLevelValue();
    }
    case blobTag:
      return Promise.resolve(
          new NomsBlob(new BlobLeafSequence(ds, new Uint8Array(chunk.data.buffer, 2))));
    default:
      throw new Error('Not implemented');
  }
}
