// @flow

import Chunk from './chunk.js';
import type Ref from './ref.js';
import {emptyRef} from './ref.js';
import RefValue from './ref-value.js';
import {default as Struct, StructMirror} from './struct.js';
import type DataStore from './data-store.js';
import type {NomsKind} from './noms-kind.js';
import {encode as encodeBase64} from './base64.js';
import {boolType, EnumDesc, stringType, StructDesc, Type, typeType} from './type.js';
import {indexTypeForMetaSequence, MetaTuple} from './meta-sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {lookupPackage, Package} from './package.js';
import {MapLeafSequence, NomsMap} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {Sequence} from './sequence.js';
import {setEncodeNomsValue} from './get-ref.js';
import {NomsBlob, BlobLeafSequence} from './blob.js';
import describeType from './describe-type.js';

const typedTag = 't ';

export class JsonArrayWriter {
  array: Array<any>;
  _ds: ?DataStore;

  constructor(ds: ?DataStore) {
    this.array = [];
    this._ds = ds;
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
        if (pkg && this._ds) {
          this._ds.writeValue(pkg);
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
    const w2 = new JsonArrayWriter(this._ds);
    const indexType = indexTypeForMetaSequence(t);
    for (let i = 0; i < v.items.length; i++) {
      const tuple = v.items[i];
      invariant(tuple instanceof MetaTuple);
      if (tuple.sequence && this._ds) {
        const child = tuple.sequence;
        this._ds.writeValue(child);
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
        invariant(v instanceof NomsBlob || v instanceof Sequence,
                  `Failed to write Blob. Invalid type: ${describeType(v)}`);
        const sequence: Sequence = v instanceof NomsBlob ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof BlobLeafSequence);
        this.writeBlob(sequence);
        break;
      case Kind.Bool:
        invariant(typeof v === 'boolean', `Failed to write Bool. Invalid type: ${describeType(v)}`);
        this.write(v);
        break;
      case Kind.String:
        invariant(typeof v === 'string',
                  `Failed to write String. Invalid type: ${describeType(v)}`);
        this.write(v);
        break;
      case Kind.Float32:
      case Kind.Float64:
        invariant(typeof v === 'number',
                `Failed to write ${t.describe()}. Invalid type: ${describeType(v)}`);
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
        invariant(typeof v === 'number',
              `Failed to write ${t.describe()}. Invalid type: ${describeType(v)}`);
        this.writeInt(v); // TODO: Verify value fits in type
        break;
      case Kind.List: {
        invariant(v instanceof NomsList || v instanceof Sequence,
                  `Failed to write List. Invalid type: ${describeType(v)}`);
        const sequence: Sequence = v instanceof NomsList ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof ListLeafSequence);
        const w2 = new JsonArrayWriter(this._ds);
        const elemType = t.elemTypes[0];
        sequence.items.forEach(sv => w2.writeValue(sv, elemType, pkg));
        this.write(w2.array);
        break;
      }
      case Kind.Map: {
        invariant(v instanceof NomsMap || v instanceof Sequence,
                  `Failed to write Map. Invalid type: ${describeType(v)}`);
        const sequence: Sequence = v instanceof NomsMap ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof MapLeafSequence);
        const w2 = new JsonArrayWriter(this._ds);
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
        invariant(v instanceof Package,
                  `Failed to write Package. Invalid type: ${describeType(v)}`);
        const w2 = new JsonArrayWriter(this._ds);
        v.types.forEach(type => w2.writeValue(type, typeType, v));
        this.write(w2.array);
        const w3 = new JsonArrayWriter(this._ds);
        v.dependencies.forEach(ref => w3.writeRef(ref));
        this.write(w3.array);
        break;
      }
      case Kind.Ref: {
        invariant(v instanceof RefValue,
                  `Failed to write Ref. Invalid type: ${describeType(v)}`);
        this.writeRef(v.targetRef);
        break;
      }
      case Kind.Set: {
        invariant(v instanceof NomsSet || v instanceof Sequence,
                  `Failed to write Set. Invalid type: ${describeType(v)}`);
        const sequence: Sequence = v instanceof NomsSet ? v.sequence : v;

        if (this.maybeWriteMetaSequence(sequence, t, pkg)) {
          break;
        }

        invariant(sequence instanceof SetLeafSequence);
        const w2 = new JsonArrayWriter(this._ds);
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
        invariant(v instanceof Type,
                  `Failed to write Type. Invalid type: ${describeType(v)}`);
        this.writeTypeAsValue(v, pkg);
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

  writeTypeAsValue(t: Type, pkg: ?Package) {
    const k = t.kind;
    this.writeKind(k);
    switch (k) {
      case Kind.Enum:
        const desc = t.desc;
        invariant(desc instanceof EnumDesc);
        this.write(t.name);
        const w2 = new JsonArrayWriter(this._ds);
        for (let i = 0; i < desc.ids.length; i++) {
          w2.write(desc.ids[i]);
        }
        this.write(w2.array);
        break;
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        const w2 = new JsonArrayWriter(this._ds);
        t.elemTypes.forEach(elem => w2.writeTypeAsValue(elem, pkg));
        this.write(w2.array);
        break;
      }
      case Kind.Struct: {
        const desc = t.desc;
        invariant(desc instanceof StructDesc);
        this.write(t.name);
        const fieldWriter = new JsonArrayWriter(this._ds);
        desc.fields.forEach(field => {
          fieldWriter.write(field.name);
          fieldWriter.writeTypeAsValue(field.t, pkg);
          fieldWriter.write(field.optional);
        });
        this.write(fieldWriter.array);
        const choiceWriter = new JsonArrayWriter(this._ds);
        desc.union.forEach(choice => {
          choiceWriter.write(choice.name);
          choiceWriter.writeTypeAsValue(choice.t, pkg);
          choiceWriter.write(choice.optional);
        });
        this.write(choiceWriter.array);
        break;
      }
      case Kind.Unresolved: {
        const pkgRef = t.packageRef;
        // When we compute the ref for the package the first time it does not have a ref.
        const isCurrentPackage = pkg && pkg.ref && pkg.ref.equals(pkgRef);
        if (isCurrentPackage) {
          this.writeRef(emptyRef);
        } else {
          this.writeRef(pkgRef);
        }
        const ordinal = t.ordinal;
        this.writeInt(ordinal);
        if (ordinal === -1) {
          this.write(t.namespace);
          this.write(t.name);
        }

        if (!isCurrentPackage) {
          const pkg = lookupPackage(pkgRef);
          if (this._ds && pkg) {
            this._ds.writeValue(pkg);
          }
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
        invariant(typeof v === 'number',
                  `Failed to write ${typeDef.describe()}. Invalid type: ${describeType(v)}`);
        this.writeEnum(v);
        break;
      case Kind.Struct: {
        invariant(v instanceof Struct,
                  `Failed to write ${typeDef.describe()}. Invalid type: ${describeType(v)}`);
        this.writeStruct(v, t, typeDef, pkg);
        break;
      }
      default:
        throw new Error('Not reached');
    }
  }

  writeBlob(seq: BlobLeafSequence) {
    // HACK: The items property is declared as Array<T> in Flow.
    invariant(seq.items instanceof Uint8Array);
    this.write(encodeBase64(seq.items));
  }

  writeStruct(s: Struct, type: Type, typeDef: Type, pkg: Package) {
    const mirror = new StructMirror(s);
    mirror.forEachField(field => {
      if (field.optional) {
        if (field.present) {
          this.writeBoolean(true);
          this.writeValue(field.value, field.type, pkg);
        } else {
          this.writeBoolean(false);
        }
      } else {
        invariant(field.present);
        this.writeValue(field.value, field.type, pkg);
      }
    });

    if (mirror.hasUnion) {
      const {unionField} = mirror;
      this.writeInt(mirror.unionIndex);
      this.writeValue(unionField.value, unionField.type, pkg);
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

function encodeEmbeddedNomsValue(v: any, t: Type, ds: ?DataStore): Chunk {
  if (v instanceof Package) {
    // if (v.dependencies.length > 0) {
    //   throw new Error('Not implemented');
    // }
  }

  const w = new JsonArrayWriter(ds);
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

export function encodeNomsValue(v: any, t: Type, ds: ?DataStore): Chunk {
  if (t.kind === Kind.Blob) {
    invariant(v instanceof NomsBlob || v instanceof Sequence);
    const sequence: BlobLeafSequence = v instanceof NomsBlob ? v.sequence : v;
    if (!sequence.isMeta) {
      return encodeTopLevelBlob(sequence);
    }
  }
  return encodeEmbeddedNomsValue(v, t, ds);
}

setEncodeNomsValue(encodeNomsValue);
