// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {equals} from './compare.js';
import {
  makeCycleType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
} from './type.js';
import Commit, {isCommitType} from './commit.js';
import Set from './set.js';
import Ref from './ref.js';
import {newStruct} from './struct.js';

suite('commit.js', () => {
  const emptyStructType = makeStructType('', {});
  test('new Commit', () => {
    function assertTypeEquals(e, a) {
      assert.isTrue(equals(a, e), `Actual: ${a.describe()}\nExpected ${e.describe()}`);
    }

    const commit = new Commit(1, new Set());
    const at = commit.type;
    const et = makeStructType('Commit', {
      meta: emptyStructType,
      parents: makeSetType(makeRefType(makeCycleType(0))),
      value: numberType,
    });
    assertTypeEquals(et, at);

    // Commiting another Number
    const commit2 = new Commit(2, new Set([new Ref(commit)]));
    const at2 = commit2.type;
    const et2 = et;
    assertTypeEquals(et2, at2);

    // Now commit a String
    const commit3 = new Commit('Hi', new Set([new Ref(commit2)]));
    const at3 = commit3.type;
    const et3 = makeStructType('Commit', {
      meta: emptyStructType,
      parents: makeSetType(makeRefType(makeStructType('Commit', {
        meta: emptyStructType,
        parents: makeSetType(makeRefType(makeCycleType(0))),
        value: makeUnionType([numberType, stringType]),
      }))),
      value: stringType,
    });
    assertTypeEquals(et3, at3);

    // Now commit a String with MetaInfo
    const meta = newStruct('Meta', {date: 'some date', number: 9});
    const metaType = makeStructType('Meta', {
      date: stringType,
      number: numberType,
    });
    const commit4 = new Commit('Hi', new Set([new Ref(commit2)]), meta);
    const at4 = commit4.type;
    const et4 = makeStructType('Commit', {
      meta: metaType,
      parents: makeSetType(makeRefType(makeStructType('Commit', {
        meta: makeUnionType([emptyStructType, metaType]),
        parents: makeSetType(makeRefType(makeCycleType(0))),
        value: makeUnionType([numberType, stringType]),
      }))),
      value: stringType,
    });
    assertTypeEquals(et4, at4);
  });

  test('Commit without meta field', () => {
    const metaCommit = newStruct('Commit', {
      value: 9,
      parents: new Set(),
      meta: newStruct('', {}),
    });
    assert.isTrue(isCommitType(metaCommit.type));

    const noMetaCommit = newStruct('Commit', {
      value: 9,
      parents: new Set(),
    });
    assert.isFalse(isCommitType(noMetaCommit.type));
  });
});
