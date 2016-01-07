// @flow

import Ref from './ref.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {Collection} from './collection.js';
import {equals} from './value.js';
import {isPrimitive} from './primitives.js';
import {OrderedSequence} from './ordered_sequence.js';

type Entry<K: valueOrPrimitive, V: valueOrPrimitive> = {
  key: K,
  value: V
};

export class NomsMap<K: valueOrPrimitive, V: valueOrPrimitive> extends Collection<OrderedSequence> {
  get chunks(): Array<Ref> {
    if (this.sequence.isMeta) {
      return super.chunks;
    }

    let chunks = [];
    this.sequence.items.forEach(entry => {
      if (!isPrimitive(entry.key)) {
        chunks.push(...entry.key.chunks);
      }
      if (!isPrimitive(entry.value)) {
        chunks.push(...entry.value.chunks);
      }
    });

    return chunks;
  }

  async has(key: K): Promise<boolean> {
    let cursor = await this.sequence.newCursorAt(this.cs, key);
    return cursor.valid && equals(cursor.getCurrentKey(), key);
  }

  async first(): Promise<?[K, V]> {
    let cursor = await this.sequence.newCursorAt(this.cs, null);
    if (!cursor.valid) {
      return undefined;
    }

    let entry = cursor.getCurrent();
    return [entry.key, entry.value];
  }

  async get(key: K): Promise<?V> {
    let cursor = await this.sequence.newCursorAt(this.cs, key);
    if (!cursor.valid) {
      return undefined;
    }

    let entry = cursor.getCurrent();
    return equals(entry.key, key) ? entry.value : undefined;
  }

  async forEach(cb: (v: V, k: K) => void): Promise<void> {
    let cursor = await this.sequence.newCursorAt(this.cs, null);
    return cursor.iter(entry => {
      cb(entry.value, entry.key);
      return false;
    });
  }

  get size(): number {
    if (this.sequence instanceof MapLeafSequence) {
      return this.sequence.items.length;
    }

    throw new Error('Not implemented');
  }
}

export class MapLeafSequence<K: valueOrPrimitive, V: valueOrPrimitive> extends
    OrderedSequence<K, Entry<K, V>> {
  getKey(idx: number): K {
    return this.items[idx].key;
  }
}
