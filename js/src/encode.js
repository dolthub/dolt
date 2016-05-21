// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct, {StructMirror} from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {encode as encodeBase64} from './base64.js';
import {StructDesc, Type, getTypeOfValue} from './type.js';
import {MetaTuple} from './meta-sequence.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import List, {ListLeafSequence} from './list.js';
import Map, {MapLeafSequence} from './map.js';
import Set, {SetLeafSequence} from './set.js';
import Sequence from './sequence.js';
import {setEncodeNomsValue} from './get-hash.js';
import Blob, {BlobLeafSequence} from './blob.js';
import {describeTypeOfValue} from './encode-human-readable.js';
import type {primitive} from './primitives.js';
import type Value from './value.js';
import type {ValueWriter} from './value-store.js';

const typedTag = 't ';

type primitiveOrArray = primitive | Array<primitiveOrArray>;

export class JsonArrayWriter {
  array: Array<primitiveOrArray>;
  _vw: ?ValueWriter;

  constructor(ds: ?ValueWriter) {
    this.array = [];
    this._vw = ds;
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

  writeUint16(n: number) {
    this.write(n);
  }

  writeKind(k: NomsKind) {
    this.write(k);
  }

  writeRef(r: Ref) {
    this.write(r.targetHash.toString());
    this.writeInt(r.height);
  }

  writeType(t: Type, parentStructTypes: Type<StructDesc>[]) {
    const k = t.kind;
    switch (k) {
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set:
        this.writeKind(k);
        t.elemTypes.forEach(elemType => this.writeType(elemType, parentStructTypes));
        break;
      case Kind.Union:
        this.writeKind(k);
        this.writeUint16(t.elemTypes.length);
        t.elemTypes.forEach(elemType => this.writeType(elemType, parentStructTypes));
        break;
      case Kind.Struct:
        this.writeStructType(t, parentStructTypes);
        break;
      default:
        invariant(isPrimitiveKind(k));
        this.writeKind(k);
    }
  }

  maybeWriteMetaSequence(v: Sequence): boolean {
    if (!v.isMeta) {
      this.write(false);
      return false;
    }

    this.write(true);
    const w2 = new JsonArrayWriter(this._vw);
    for (let i = 0; i < v.items.length; i++) {
      const tuple: MetaTuple = v.items[i];
      invariant(tuple instanceof MetaTuple);
      const child = tuple.child;
      if (child && this._vw) {
        this._vw.writeValue(child);
      }
      w2.writeValue(tuple.ref);
      w2.writeValue(tuple.value);
      w2.writeInt(tuple.numLeaves);
    }
    this.write(w2.array);
    return true;
  }

  writeValue(v: Value) {
    const t = getTypeOfValue(v);
    this.writeType(t, []);
    switch (t.kind) {
      case Kind.Blob: {
        invariant(v instanceof Blob,
                  () => `Failed to write Blob. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence = v.sequence;
        if (this.maybeWriteMetaSequence(sequence)) {
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
        invariant(v instanceof List,
                  () => `Failed to write List. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence = v.sequence;
        if (this.maybeWriteMetaSequence(sequence)) {
          break;
        }

        invariant(sequence instanceof ListLeafSequence);
        const w2 = new JsonArrayWriter(this._vw);
        sequence.items.forEach(sv => w2.writeValue(sv));
        this.write(w2.array);
        break;
      }
      case Kind.Map: {
        invariant(v instanceof Map,
                  () => `Failed to write Map. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence = v.sequence;
        if (this.maybeWriteMetaSequence(sequence)) {
          break;
        }

        invariant(sequence instanceof MapLeafSequence);
        const w2 = new JsonArrayWriter(this._vw);
        sequence.items.forEach(entry => {
          w2.writeValue(entry[0]);
          w2.writeValue(entry[1]);
        });
        this.write(w2.array);
        break;
      }
      case Kind.Ref: {
        invariant(v instanceof Ref,
                  () => `Failed to write Ref. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeRef(v);
        break;
      }
      case Kind.Set: {
        invariant(v instanceof Set,
                  () => `Failed to write Set. Invalid type: ${describeTypeOfValue(v)}`);
        const sequence = v.sequence;
        if (this.maybeWriteMetaSequence(sequence)) {
          break;
        }

        invariant(sequence instanceof SetLeafSequence);
        const w2 = new JsonArrayWriter(this._vw);
        sequence.items.forEach(v => {
          w2.writeValue(v);
        });
        this.write(w2.array);
        break;
      }
      case Kind.Type: {
        invariant(v instanceof Type,
                  () => `Failed to write Type. Invalid type: ${describeTypeOfValue(v)}`);
        this.writeType(v, []);
        break;
      }
      case Kind.Value: {
        this.writeValue(v);
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
    const fieldWriter = new JsonArrayWriter(this._vw);
    desc.forEachField((name: string, type: Type) => {
      fieldWriter.write(name);
      fieldWriter.writeType(type, parentStructTypes);
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
      this.writeValue(field.value);
    });
  }
}

function encodeEmbeddedNomsValue(v: Value, vw: ?ValueWriter): Chunk {
  const w = new JsonArrayWriter(vw);
  w.writeValue(v);
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

export function encodeNomsValue(v: Value, vw: ?ValueWriter): Chunk {
  const t = getTypeOfValue(v);
  if (t.kind === Kind.Blob) {
    invariant(v instanceof Blob);
    const sequence = v.sequence;
    if (!sequence.isMeta) {
      invariant(sequence instanceof BlobLeafSequence);
      return encodeTopLevelBlob(sequence);
    }
  }

  return encodeEmbeddedNomsValue(v, vw);
}

setEncodeNomsValue(encodeNomsValue);
