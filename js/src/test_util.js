// @flow

import {notNull} from './assert.js';
import {AsyncIterator} from './async_iterator.js';

export async function flatten<T>(iter: AsyncIterator<T>): Promise<Array<T>> {
  const values = [];
  for (let next = await iter.next(); !next.done; next = await iter.next()) {
    values.push(notNull(next.value));
  }
  return values;
}
