// @flow

export type AsyncIteratorResult<T> = {
  done: true,
  value?: void, // It would be better to leave out |value| entirely, but Flow doesn't like it.
} | {
  done: false,
  value: T,
};

export class AsyncIterator<T> {
  next(): Promise<AsyncIteratorResult<T>> {
    throw new Error('override');
  }
  return(): Promise<AsyncIteratorResult<T>> {
    throw new Error('override');
  }
}
