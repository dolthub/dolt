// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {invariant, notNull} from './assert.js';

class Foo {
  doNothing() {}
}

function doSomething() {}

suite('assert', () => {
  test('invariant', () => {
    invariant(true);
    assert.throws(() => {
      invariant(false);
    });
  });


  test('notNull', () => {
    let t: ?Foo = null;
    assert.throws(() => {
      notNull(t);
    });

    t = new Foo();
    doSomething(); // might have nullified t;
    const t2: Foo = notNull(t); // shouldn't throw
    t2.doNothing();
  });
});
