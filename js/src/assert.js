// @flow

export function invariant(exp: any, message: string = 'Invariant violated') {
  if (process.env.NODE_ENV === 'production') return;
  if (!exp) throw new Error(message);
}

export function notNull<T>(v: ?T): T {
  invariant(v !== null && v !== undefined, 'Unexpected null value');
  return v;
}
