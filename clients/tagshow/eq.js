/* @flow */

import {Ref} from 'noms';

export default function eq(a: any, b: any) : boolean {
  // Babel bug: https://github.com/babel/babel/issues/3046
  let i;
  if (a === b) return true;
  let ta = typeof a;
  let tb = typeof b;
  if (ta !== tb) return false;
  if (a === null || b === null) return false;
  if (ta !== 'object') return false;
  if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) return false;
  if (a instanceof Ref) {
    return a.equals(b);
  }
  // https://github.com/attic-labs/noms/issues/615
  // let ar = a.ref;
  // let br = b.ref;
  // if (ar && br) {
  //   return eq(ar, br);
  // }
  if (a instanceof Array) {
    if (a.length !== b.length) return false;
    for (i = 0; i < a.length; i++) {
      if (!eq(a[i], b[i])) return false;
    }
    return true;
  }
  if (a instanceof Set) {
    if (a.size !== b.size) return false;
    // has uses object identity
    return eq([...a].sort(), [...b].sort());
  }
  if (a instanceof Map) {
    if (a.size !== b.size) return false;
    // get uses object identity
    let compare = (a, b) => {
      if (eq(a[0], b[0])) return 0;
      if (a[0] < b[0]) return -1;
      return 1;
    };
    return eq([...a].sort(compare), [...b].sort(compare));
  }

  let ka = Object.keys(a);
  let kb = Object.keys(b);
  if (ka.length !== kb.length) return false;
  for (i = 0; i < ka.length; i++) {
    if (!eq(a[ka[i]], b[ka[i]])) return false;
  }
  return true;
}
