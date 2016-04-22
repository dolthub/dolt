// @flow

import {
  CompoundDesc,
  Field,
  makeCompoundType,
  makeStructType,
  makeType,
  PrimitiveDesc,
  StructDesc,
  Type,
  UnresolvedDesc,
} from './type.js';
import {Package} from './package.js';
import {invariant, notNull} from './assert.js';

/**
 * Goes through the type and returns a new type where all the empty refs have been replaced by
 * the package ref.
 */
export default function fixupType(t: Type, pkg: ?Package): Type {
  const desc = t.desc;

  if (desc instanceof CompoundDesc) {
    let changed = false;
    const newTypes = desc.elemTypes.map(t => {
      const newT = fixupType(t, pkg);
      if (newT === t) {
        return t;
      }
      changed = true;
      return newT;
    });

    return changed ? makeCompoundType(t.kind, ...newTypes) : t;
  }

  if (desc instanceof UnresolvedDesc) {
    if (t.hasPackageRef) {
      return t;
    }

    return makeType(notNull(pkg).ref, t.ordinal);
  }

  if (desc instanceof StructDesc) {
    let changed = false;
    const fixField = f => {
      const newT = fixupType(f.t, pkg);
      if (newT === t) {
        return f;
      }
      changed = true;
      return new Field(f.name, newT, f.optional);
    };

    const newFields = desc.fields.map(fixField);
    const newUnion = desc.union.map(fixField);
    return changed ? makeStructType(t.name, newFields, newUnion) : t;
  }

  invariant(desc instanceof PrimitiveDesc);
  return t;
}
