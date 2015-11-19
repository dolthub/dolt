/* @flow */

export function invariant(exp: boolean, message: string = 'Invariant violated') {
  if (!exp) {
    throw new Error(message);
  }
}

export function notNull<T>(v: ?T): T {
  invariant(v !== null && v !== undefined, 'Unexpected null value');
  return v;
}
