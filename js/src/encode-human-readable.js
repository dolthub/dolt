// @flow

import {StructDesc, BackRefDesc, CompoundDesc} from './type.js';
import type {Field, Type} from './type.js';
import {Kind, kindToString} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import {invariant} from './assert.js';
import type {Value} from './value.js';
import {ValueBase} from './value.js';

export interface StringWriter {
  write(s: string): void;
}

class Writer {
  ind: number;
  w: StringWriter;
  lineLength: number;

  constructor(w: StringWriter) {
    this.ind = 0;
    this.w = w;
    this.lineLength = 0;
  }

  maybeWriteIndentation() {
    if (this.lineLength === 0) {
      for (let i = 0; i < this.ind; i++) {
        this.w.write('  ');
      }
      this.lineLength = 2 * this.ind;
    }
  }

  write(s: string) {
    this.maybeWriteIndentation();
    this.w.write(s);
    this.lineLength += s.length;
  }

  indent() {
    this.ind++;
  }

  outdent() {
    this.ind--;
  }

  newLine() {
    this.write('\n');
    this.lineLength = 0;
  }

  writeKind(k: NomsKind) {
    this.write(kindToString(k));
  }
}

export class TypeWriter {
  _w: Writer;

  constructor(w: StringWriter) {
    this._w = new Writer(w);
  }

  writeType(t: Type) {
    this._writeType(t, []);
  }

  _writeType(t: Type, backRefs: Type[]) {
    switch (t.kind) {
      case Kind.Blob:
      case Kind.Bool:
      case Kind.Number:
      case Kind.String:
      case Kind.Type:
      case Kind.Value:
        this._w.writeKind(t.kind);
        break;
      case Kind.List:
      case Kind.Ref:
      case Kind.Set:
        this._w.writeKind(t.kind);
        this._w.write('<');
        invariant(t.desc instanceof CompoundDesc);
        this._writeType(t.desc.elemTypes[0], backRefs);
        this._w.write('>');
        break;
      case Kind.Map: {
        this._w.writeKind(t.kind);
        this._w.write('<');
        invariant(t.desc instanceof CompoundDesc);
        const [keyType, valueType] = t.desc.elemTypes;
        this._writeType(keyType, backRefs);
        this._w.write(', ');
        this._writeType(valueType, backRefs);
        this._w.write('>');
        break;
      }
      case Kind.Struct:
        this._writeStructType(t, backRefs);
        break;
      case Kind.BackRef:
        invariant(t.desc instanceof BackRefDesc);
        this._writeBackRef(t.desc.value);
        break;
      default:
        throw new Error('unreachable');
    }
  }

  _writeBackRef(i: number) {
    this._w.write(`BackRef<${i}>`);
  }

  _writeStructType(t: Type, backRefs: Type[]) {
    const idx = backRefs.indexOf(t);
    if (idx !== -1) {
      this._writeBackRef(backRefs.length - idx - 1);
      return;
    }
    backRefs = backRefs.concat(t);

    const desc = t.desc;
    invariant(desc instanceof StructDesc);
    this._w.write('struct ');
    this._w.write(desc.name);
    this._w.write(' {');
    this._w.indent();

    desc.fields.forEach((f: Field, i: number) => {
      if (i === 0) {
        this._w.newLine();
      }
      this._w.write(f.name);
      this._w.write(': ');
      if (f.optional) {
        this._w.write('optional ');
      }
      this._writeType(f.t, backRefs);
      this._w.newLine();
    });

    this._w.outdent();
    this._w.write('}');
  }
}

export function describeType(t: Type) {
  let s = '';
  const w = new TypeWriter({
    write(s2: string) {
      s += s2;
    },
  });
  w.writeType(t);
  return s;
}

export function describeTypeOfValue(v: Value) {
  const t = typeof v;
  if (t === 'object') {
    if (v === null) {
      return 'null';
    }
    if (v instanceof ValueBase) {
      return describeType(v.type);
    }
  }
  return t;
}
