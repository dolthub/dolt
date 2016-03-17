// @flow

import {notNull} from './assert.js';
import {AsyncIterator} from './async-iterator.js';

export async function flatten<T>(iter: AsyncIterator<T>): Promise<Array<T>> {
  const values = [];
  for (let next = await iter.next(); !next.done; next = await iter.next()) {
    values.push(notNull(next.value));
  }
  return values;
}

export async function flattenParallel<T>(iter: AsyncIterator<T>, count: number): Promise<Array<T>> {
  const promises = [];
  for (let i = 0; i < count; i++) {
    promises.push(iter.next());
  }
  const results = await Promise.all(promises);
  return results.map(res => notNull(res.value));
}
