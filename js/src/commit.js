// @flow

import type Struct from './struct.js';
import type {valueOrPrimitive} from './value.js';
import type RefValue from './ref-value.js';
import type {NomsSet} from './set.js';

export interface Commit extends Struct {
  value: valueOrPrimitive;  // readonly
  setValue(value: valueOrPrimitive): Commit;
  parents: NomsSet<RefValue<Commit>>;  // readonly
  setParents(value: NomsSet<RefValue<Commit>>): Commit;
}
