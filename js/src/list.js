  // @flow

import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {Collection} from './collection.js';
import {IndexedSequence} from './indexed_sequence.js';

export class NomsList<T: valueOrPrimitive> extends Collection<IndexedSequence> {
  async get(idx: number): Promise<T> {
    // TODO (when |length| works) invariant(idx < this.length, idx + ' >= ' + this.length);
    let cursor = await this.sequence.newCursorAt(this.cs, idx);
    return cursor.getCurrent();
  }

  async forEach(cb: (v: T, i: number) => void): Promise<void> {
    let cursor = await this.sequence.newCursorAt(this.cs, 0);
    return cursor.iter((v, i) => {
      cb(v, i);
      return false;
    });
  }

  get length(): number {
    if (this.sequence instanceof ListLeafSequence) {
      return this.sequence.items.length;
    }

    throw new Error('not implemented');
  }
}

export class ListLeafSequence<T: valueOrPrimitive> extends IndexedSequence<T> {
  getOffset(idx: number): number {
    return idx;
  }
}
