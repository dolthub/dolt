// @flow

import type Struct from './struct.js';
import type {valueOrPrimitive} from './value.js';
import type RefValue from './ref-value.js';
import type Set from './set.js';

export interface Commit extends Struct {
  value: valueOrPrimitive;  // readonly
  setValue(value: valueOrPrimitive): Commit;
  parents: Set<RefValue<Commit>>;  // readonly
  setParents(value: Set<RefValue<Commit>>): Commit;
}
