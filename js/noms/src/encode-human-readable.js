// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {getTypeOfValue, CompoundDesc, CycleDesc} from './type.js';
import type {Type} from './type.js';
import {Kind, kindToString} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import {invariant} from './assert.js';
import type Value from './value.js';

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

  writeType(t: Type<any>) {
    this._writeType(t, []);
  }

  _writeType(t: Type<any>, parentStructTypes: Type<any>[]) {
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
      case Kind.Map:
        this._w.writeKind(t.kind);
        this._w.write('<');
        invariant(t.desc instanceof CompoundDesc);
        t.desc.elemTypes.forEach((t, i) => {
          if (t.kind === Kind.Union && t.desc.elemTypes.length === 0) {
            // If one of the element types is an empty union all the other element types must also
            // be empty union types.
            return;
          }
          if (i !== 0) {
            this._w.write(', ');
          }
          this._writeType(t, parentStructTypes);
        });
        this._w.write('>');
        break;
      case Kind.Union:
        invariant(t.desc instanceof CompoundDesc);
        t.desc.elemTypes.forEach((t, i) => {
          if (i !== 0) {
            this._w.write(' | ');
          }
          this._writeType(t, parentStructTypes);
        });
        break;

      case Kind.Struct:
        this._writeStructType(t, parentStructTypes);
        break;
      case Kind.Cycle:
        invariant(t.desc instanceof CycleDesc);
        this._w.write(`Cycle<${t.desc.level}>`);
        break;
      default:
        throw new Error('unreachable');
    }
  }

  _writeCycle(i: number) {
    this._w.write(`Cycle<${i}>`);
  }

  _writeStructType(t: Type<any>, parentStructTypes: Type<any>[]) {
    const idx = parentStructTypes.indexOf(t);
    if (idx !== -1) {
      this._writeCycle(parentStructTypes.length - idx - 1);
      return;
    }
    parentStructTypes.push(t);

    const desc = t.desc;
    this._w.write('struct ');
    if (desc.name !== '') {
      this._w.write(`${desc.name} `);
    }
    this._w.write('{');
    this._w.indent();

    let first = true;
    desc.forEachField((name: string, type: Type<any>) => {
      if (first) {
        this._w.newLine();
        first = false;
      }
      this._w.write(name);
      this._w.write(': ');
      this._writeType(type, parentStructTypes);
      this._w.write(',');
      this._w.newLine();
    });

    this._w.outdent();
    this._w.write('}');
    parentStructTypes.pop(t);
  }
}

export function describeType(t: Type<any>): string {
  let s = '';
  const w = new TypeWriter({
    write(s2: string) {
      s += s2;
    },
  });
  w.writeType(t);
  return s;
}

export function describeTypeOfValue(v: Value): string {
  if (v === null) {
    return 'null';
  }

  return describeType(getTypeOfValue(v));
}
