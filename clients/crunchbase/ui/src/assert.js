// @flow

export function invariant(exp: any, message: string = 'Invariant violated') {
  if (!exp) {
    throw new Error(message);
  }
}
