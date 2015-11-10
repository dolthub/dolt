/* @flow */

'use strict';

export function invariant(exp: boolean, message: string = 'Invariant violated') {
  if (!exp) {
    throw new Error(message);
  }
}
