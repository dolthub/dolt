// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct from './struct.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {encode as encodeBase64} from './base64.js';
import {boolType, EnumDesc, makePrimitiveType, stringType, StructDesc, Type} from './type.js';
import {indexTypeForMetaSequence, MetaTuple} from './meta_sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {lookupPackage, Package} from './package.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {Sequence} from './sequence.js';
import {setEncodeNomsValue} from './get_ref.js';

const typedTag = 't ';

class JsonArrayWriter {
  array: Array<any>;
  _cs: ?ChunkStore;

  constructor(cs: ?ChunkStore) {
    this.array = [];
    this._cs = cs;
  }

  write(v: any) {
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

  writeKind(k: NomsKind) {
    this.write(k);
  }

  writeRef(r: Ref) {
    this.write(r.toString());
  }

  writeTypeAsTag(t: Type) {
    const k = t.kind;
    this.writeKind(k);
    switch (k) {
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Unreachable');
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        t.elemTypes.forEach(elemType => this.writeTypeAsTag(elemType));
        break;
      }
      case Kind.Unresolved: {
        const pkgRef = t.packageRef;
        invariant(!pkgRef.isEmpty());
        this.writeRef(pkgRef);
        this.writeInt(t.ordinal);

        const pkg = lookupPackage(pkgRef);
        if (pkg && this._cs) {
          writeValue(pkg, pkg.type, this._cs);
        }
        break;
      }
    }
  }

  writeTopLevel(t: Type, v: any) {
    this.writeTypeAsTag(t);
    this.writeValue(v, t);
  }

  maybeWriteMetaSequence(v: Sequence, t: Type, pkg: ?Package): boolean {
    if (!v.isMeta) {
      this.write(false);
      return false;
    }

    this.write(true);
    const w2 = new JsonArrayWriter(this._cs);
    const indexType = indexTypeForMetaSequence(t);
    for (let i = 0; i < v.items.length; i++) {
      const tuple = v.items[i];
      invariant(tuple instanceof MetaTuple);
      if (tuple.sequence && this._cs) {
        const child = tuple.sequence;
        writeValue(child, child.type, this._cs);
      }
      w2.writeRef(tuple.ref);
      w2.writeValue(tuple.value, indexType, pkg);
    }
    this.write(w2.array);
    return true;
  }

  writeValue(v: any, t: Type, pkg: ?Package) {
    switch (t.kind) {
      case Kind.Blob:
        this.write(false);
        // TODO: When CompoundBlob is implemented...
        // invariant(v instanceof Sequence);
        // if (this.maybeWriteMetaSequence(v, t, pkg)) {
        //   break;
        // }

        this.writeBlob(v);
        break;
      case Kind.Bool:
      case Kind.String:
        this.write(v);
        break;
      case Kind.Float32:
      case Kind.Float64:
        this.writeFloat(v); // TODO: Verify value fits in type
        break;
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
        this.writeInt(v); // TODO: Verify value fits in type
        break;
      case Kind.List: {
        invariant(v instanceof NomsList || v instanceof Sequence);
        const sequence: Sequence = v instanceof NomsList ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof ListLeafSequence);
        const w2 = new JsonArrayWriter(this._cs);
        const elemType = t.elemTypes[0];
        sequence.items.forEach(sv => w2.writeValue(sv, elemType, pkg));
        this.write(w2.array);
        break;
      }
      case Kind.Map: {
        invariant(v instanceof NomsMap || v instanceof Sequence);
        const sequence: Sequence = v instanceof NomsMap ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof MapLeafSequence);
        const w2 = new JsonArrayWriter(this._cs);
        const keyType = t.elemTypes[0];
        const valueType = t.elemTypes[1];
        sequence.items.forEach(entry => {
          w2.writeValue(entry.key, keyType, pkg);
          w2.writeValue(entry.value, valueType, pkg);
        });
        this.write(w2.array);
        break;
      }
      case Kind.Package: {
        invariant(v instanceof Package);
        const ptr = makePrimitiveType(Kind.Type);
        const w2 = new JsonArrayWriter(this._cs);
        v.types.forEach(type => w2.writeValue(type, ptr, pkg));
        this.write(w2.array);
        const w3 = new JsonArrayWriter(this._cs);
        v.dependencies.forEach(ref => w3.writeRef(ref));
        this.write(w3.array);
        break;
      }
      case Kind.Ref: {
        // TODO: This is not aligned with Go. In Go we have a dedicated Value
        // for refs.
        invariant(v instanceof Ref);
        this.writeRef(v);
        break;
      }
      case Kind.Set: {
        invariant(v instanceof NomsSet || v instanceof Sequence);
        const sequence: Sequence = v instanceof NomsSet ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof SetLeafSequence);
        const w2 = new JsonArrayWriter(this._cs);
        const elemType = t.elemTypes[0];
        const elems = [];
        sequence.items.forEach(v => {
          elems.push(v);
        });
        elems.forEach(elem => w2.writeValue(elem, elemType, pkg));
        this.write(w2.array);
        break;
      }
      case Kind.Type: {
        invariant(v instanceof Type);
        this.writeTypeAsValue(v);
        break;
      }
      case Kind.Unresolved: {
        if (t.hasPackageRef) {
          pkg = lookupPackage(t.packageRef);
        }
        pkg = notNull(pkg);
        this.writeUnresolvedKindValue(v, t, pkg);
        break;
      }
      case Kind.Value: {
        const valueType = getTypeOfValue(v);
        this.writeTypeAsTag(valueType);
        this.writeValue(v, valueType, pkg);
        break;
      }
      default:
        throw new Error(`Not implemented: ${t.kind} ${v}`);
    }
  }

  writeTypeAsValue(t: Type) {
    const k = t.kind;
    this.writeKind(k);
    switch (k) {
      case Kind.Enum:
        const desc = t.desc;
        invariant(desc instanceof EnumDesc);
        this.write(t.name);
        const w2 = new JsonArrayWriter(this._cs);
        for (let i = 0; i < desc.ids.length; i++) {
          w2.write(desc.ids[i]);
        }
        this.write(w2.array);
        break;
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        const w2 = new JsonArrayWriter(this._cs);
        t.elemTypes.forEach(elem => w2.writeTypeAsValue(elem));
        this.write(w2.array);
        break;
      }
      case Kind.Struct: {
        const desc = t.desc;
        invariant(desc instanceof StructDesc);
        this.write(t.name);
        const fieldWriter = new JsonArrayWriter(this._cs);
        desc.fields.forEach(field => {
          fieldWriter.write(field.name);
          fieldWriter.writeTypeAsValue(field.t);
          fieldWriter.write(field.optional);
        });
        this.write(fieldWriter.array);
        const choiceWriter = new JsonArrayWriter(this._cs);
        desc.union.forEach(choice => {
          choiceWriter.write(choice.name);
          choiceWriter.writeTypeAsValue(choice.t);
          choiceWriter.write(choice.optional);
        });
        this.write(choiceWriter.array);
        break;
      }
      case Kind.Unresolved: {
        const pkgRef = t.packageRef;
        this.writeRef(pkgRef);
        const ordinal = t.ordinal;
        this.writeInt(ordinal);
        if (ordinal === -1) {
          this.write(t.namespace);
          this.write(t.name);
        }

        const pkg = lookupPackage(pkgRef);
        if (pkg && this._cs) {
          writeValue(pkg, pkg.type, this._cs);
        }

        break;
      }

      default: {
        invariant(isPrimitiveKind(k));
      }
    }
  }

  writeUnresolvedKindValue(v: any, t: Type, pkg: Package) {
    const typeDef = pkg.types[t.ordinal];
    switch (typeDef.kind) {
      case Kind.Enum:
        invariant(typeof v === 'number');
        this.writeEnum(v);
        break;
      case Kind.Struct: {
        invariant(v instanceof Struct);
        this.writeStruct(v, t, typeDef, pkg);
        break;
      }
      default:
        throw new Error('Not reached');
    }
  }

  writeBlob(v: ArrayBuffer) {
    this.write(encodeBase64(v));
  }

  writeStruct(s: Struct, type: Type, typeDef: Type, pkg: Package) {
    const desc = typeDef.desc;
    invariant(desc instanceof StructDesc);
    for (let i = 0; i < desc.fields.length; i++) {
      const field = desc.fields[i];
      const fieldValue = s.get(field.name);
      if (field.optional) {
        if (fieldValue !== undefined) {
          this.writeBoolean(true);
          this.writeValue(fieldValue, field.t, pkg);
        } else {
          this.writeBoolean(false);
        }
      } else {
        invariant(fieldValue !== undefined);
        this.writeValue(s.get(field.name), field.t, pkg);
      }
    }

    if (s.hasUnion) {
      const unionField = notNull(s.unionField);
      this.writeInt(s.unionIndex);
      this.writeValue(s.get(unionField.name), unionField.t, pkg);
    }
  }

  writeEnum(v: number) {
    this.writeInt(v);
  }
}

function getTypeOfValue(v: any): Type {
  switch (typeof v) {
    case 'object':
      return v.type;
    case 'string':
      return stringType;
    case 'boolean':
      return boolType;
    case 'number':
      throw new Error('Encoding untagged numbers is not supported');
    default:
      throw new Error('Unknown type');
  }
}

function encodeEmbeddedNomsValue(v: any, t: Type, cs: ?ChunkStore): Chunk {
  if (v instanceof Package) {
    // if (v.dependencies.length > 0) {
    //   throw new Error('Not implemented');
    // }
  }

  const w = new JsonArrayWriter(cs);
  w.writeTopLevel(t, v);
  return Chunk.fromString(typedTag + JSON.stringify(w.array));
}

// Top level blobs are not encoded using JSON but prefixed with 'b ' followed
// by the raw bytes.
function encodeTopLevelBlob(v: ArrayBuffer): Chunk {
  const data = new Uint8Array(2 + v.byteLength);
  const view = new DataView(v);
  data[0] = 98;  // 'b'
  data[1] = 32;  // ' '
  for (let i = 0; i < view.byteLength; i++) {
    data[i + 2] = view.getUint8(i);
  }
  return new Chunk(data);
}

function encodeNomsValue(v: any, t: Type, cs: ?ChunkStore): Chunk {
  if (t.kind === Kind.Blob) {
    invariant(v instanceof ArrayBuffer);
    return encodeTopLevelBlob(v);
  }
  return encodeEmbeddedNomsValue(v, t, cs);
}

function writeValue(v: any, t: Type, cs: ChunkStore): Ref {
  const chunk = encodeNomsValue(v, t, cs);
  invariant(!chunk.isEmpty());
  cs.put(chunk);
  return chunk.ref;
}

export {encodeNomsValue, JsonArrayWriter, writeValue};

setEncodeNomsValue(encodeNomsValue);
