// @flow

import Chunk from './chunk.js';
import type Ref from './ref.js';
import RefValue from './ref-value.js';
import {default as Struct, StructMirror} from './struct.js';
import type Database from './database.js';
import type {NomsKind} from './noms-kind.js';
import {encode as encodeBase64} from './base64.js';
import {StructDesc, Type, getTypeOfValue} from './type.js';
import {indexTypeForMetaSequence, MetaTuple} from './meta-sequence.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {Sequence} from './sequence.js';
import {setEncodeNomsValue} from './get-ref.js';
import {NomsBlob, BlobLeafSequence} from './blob.js';
import {describeTypeOfValue} from './encode-human-readable.js';
import type {primitive} from './primitives.js';
import type {valueOrPrimitive} from './value.js';
import {IndexedSequence} from './indexed-sequence.js';

const typedTag = 't ';

type primitiveOrArray = primitive | Array<primitiveOrArray>;

export class JsonArrayWriter {
  array: Array<primitiveOrArray>;
  _db: ?Database;

  constructor(db: ?Database) {
    this.array = [];
    this._db = db;
  }

  write(v: primitiveOrArray) {
    this.array.push(v);
  }

  writeBoolean(b: boolean) {
    this.write(b);
  }

  writeFloat(n: number) {
    if (n < 1e20) {
      this.write(n.toString(10));
    } else {
      this.write(n.toExponential());
    }
  }

  writeInt(n: number) {
    this.write(n.toFixed(0));
  }

  writeUint8(n: number) {
    this.write(n);
  }

  writeKind(k: NomsKind) {
    this.write(k);
  }

  writeRef(r: Ref) {
    this.write(r.toString());
  }

  writeTypeAsTag(t: Type, parentStructTypes: Type<StructDesc>[]) {
    const k = t.kind;
    switch (k) {
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set:
        this.writeKind(k);
        t.elemTypes.forEach(elemType => this.writeTypeAsTag(elemType, parentStructTypes));
        break;
      case Kind.Struct:
        this.writeStructType(t, parentStructTypes);
        break;
      default:
        this.writeKind(k);
    }
  }

  writeTopLevel(t: Type, v: valueOrPrimitive) {
    this.writeTypeAsTag(t, []);
    this.writeValue(v, t);
  }

  maybeWriteMetaSequence(v: Sequence, t: Type): boolean {
    if (!v.isMeta) {
      this.write(false);
      return false;
    }

    this.write(true);
    const w2 = new JsonArrayWriter(this._db);
    const indexType = indexTypeForMetaSequence(t);
    for (let i = 0; i < v.items.length; i++) {
      const tuple = v.items[i];
      invariant(tuple instanceof MetaTuple);
      if (tuple.sequence && this._db) {
        const child = tuple.sequence;
        this._db.writeValue(child);
      }
      w2.writeRef(tuple.ref);
      w2.writeValue(tuple.value, indexType);
      w2.writeInt(tuple.numLeaves);
    }
    this.write(w2.array);
    return true;
  }

  writeValue(v: valueOrPrimitive, t: Type) {
    switch (t.kind) {
      case Kind.Blob: {
        invariant(v instanceof NomsBlob || v instanceof Sequence,
                  () => `Failed to write Blob. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence: Sequence = v instanceof NomsBlob ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t)) {
          break;
        }

        invariant(sequence instanceof BlobLeafSequence);
        this.writeBlob(sequence);
        break;
      }
      case Kind.Bool:
        invariant(typeof v === 'boolean',
                  () => `Failed to write Bool. Invalid type: ${describeTypeOfValue(v)}`);
        this.write(v);
        break;
      case Kind.String:
        invariant(typeof v === 'string',
                  () => `Failed to write String. Invalid type: ${describeTypeOfValue(v)}`);
        this.write(v);
        break;
      case Kind.Number:
        invariant(typeof v === 'number',
                  () => `Failed to write Number. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeFloat(v);
        break;
      case Kind.List: {
        invariant(v instanceof NomsList || v instanceof Sequence,
                  () => `Failed to write List. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence: Sequence = v instanceof NomsList ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t)) {
          break;
        }

        invariant(sequence instanceof ListLeafSequence);
        const w2 = new JsonArrayWriter(this._db);
        const elemType = t.elemTypes[0];
        sequence.items.forEach(sv => w2.writeValue(sv, elemType));
        this.write(w2.array);
        break;
      }
      case Kind.Map: {
        invariant(v instanceof NomsMap || v instanceof Sequence,
                  () => `Failed to write Map. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence: Sequence = v instanceof NomsMap ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t)) {
          break;
        }

        invariant(sequence instanceof MapLeafSequence);
        const w2 = new JsonArrayWriter(this._db);
        const keyType = t.elemTypes[0];
        const valueType = t.elemTypes[1];
        sequence.items.forEach(entry => {
          w2.writeValue(entry.key, keyType);
          w2.writeValue(entry.value, valueType);
        });
        this.write(w2.array);
        break;
      }
      case Kind.Ref: {
        invariant(v instanceof RefValue,
                  () => `Failed to write Ref. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeRef(v.targetRef);
        break;
      }
      case Kind.Set: {
        invariant(v instanceof NomsSet || v instanceof Sequence,
                  () => `Failed to write Set. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence: Sequence = v instanceof NomsSet ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t)) {
          break;
        }

        invariant(sequence instanceof SetLeafSequence);
        const w2 = new JsonArrayWriter(this._db);
        const elemType = t.elemTypes[0];
        const elems = [];
        sequence.items.forEach(v => {
          elems.push(v);
        });
        elems.forEach(elem => w2.writeValue(elem, elemType));
        this.write(w2.array);
        break;
      }
      case Kind.Type: {
        invariant(v instanceof Type,
                  () => `Failed to write Type. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeTypeAsValue(v, []);
        break;
      }
      case Kind.Value: {
        const valueType = getTypeOfValue(v);
        this.writeTypeAsTag(valueType, []);
        this.writeValue(v, valueType);
        break;
      }
      case Kind.Struct:
        invariant(v instanceof Struct,
                  () => `Failed to write Struct. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeStruct(v);
        break;
      default:
        throw new Error(`Not implemented: ${t.kind} ${v}`);
    }
  }

  writeTypeAsValue(t: Type, parentStructTypes: Type<StructDesc>[]) {
    const k = t.kind;
    switch (k) {
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        this.writeKind(k);
        const w2 = new JsonArrayWriter(this._db);
        t.elemTypes.forEach(elem => w2.writeTypeAsValue(elem, parentStructTypes));
        this.write(w2.array);
        break;
      }
      case Kind.Struct: {
        this.writeStructType(t, parentStructTypes);
        break;
      }
      default:
        invariant(isPrimitiveKind(k));
        this.writeKind(k);
    }
  }

  writeStructType(t: Type<StructDesc>, parentStructTypes: Type<StructDesc>[]) {
    const i = parentStructTypes.indexOf(t);
    if (i !== -1) {
      this.writeParent(parentStructTypes.length - i - 1);
      return;
    }

    parentStructTypes.push(t);
    const desc = t.desc;
    this.writeKind(t.kind);
    this.write(t.name);
    const fieldWriter = new JsonArrayWriter(this._db);
    desc.fields.forEach(field => {
      fieldWriter.write(field.name);
      fieldWriter.writeTypeAsTag(field.type, parentStructTypes);
    });
    this.write(fieldWriter.array);
    parentStructTypes.pop();
  }

  writeParent(i: number) {
    this.write(Kind.Parent);
    this.writeUint8(i);
  }

  writeBlob(seq: BlobLeafSequence) {
    // HACK: The items property is declared as Array<T> in Flow.
    invariant(seq.items instanceof Uint8Array);
    this.write(encodeBase64(seq.items));
  }

  writeStruct(s: Struct) {
    const mirror = new StructMirror(s);
    mirror.forEachField(field => {
      this.writeValue(field.value, field.type);
    });
  }
}

function encodeEmbeddedNomsValue(v: valueOrPrimitive, t: Type, db: ?Database): Chunk {
  const w = new JsonArrayWriter(db);
  w.writeTopLevel(t, v);
  return Chunk.fromString(typedTag + JSON.stringify(w.array));
}

// Top level blobs are not encoded using JSON but prefixed with 'b ' followed
// by the raw bytes.
function encodeTopLevelBlob(sequence: BlobLeafSequence): Chunk {
  const arr = sequence.items;
  const data = new Uint8Array(2 + arr.length);
  data[0] = 98;  // 'b'
  data[1] = 32;  // ' '
  for (let i = 0; i < arr.length; i++) {
    data[i + 2] = arr[i];
  }
  return new Chunk(data);
}

export function encodeNomsValue(v: valueOrPrimitive, t: Type, db: ?Database): Chunk {
  if (t.kind === Kind.Blob) {
    invariant(v instanceof NomsBlob || v instanceof IndexedSequence);
    const sequence = v instanceof NomsBlob ? v.sequence : v;
    if (!sequence.isMeta) {
      invariant(sequence instanceof BlobLeafSequence);
      return encodeTopLevelBlob(sequence);
    }
  }
  return encodeEmbeddedNomsValue(v, t, db);
}

setEncodeNomsValue(encodeNomsValue);
