// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {assert} from 'chai';

import jsonToNoms from './json-convert.js';
import {equals} from './compare.js';
import List from './list.js';
import {newStruct} from './struct.js';

suite('jsonToNoms', () => {
  test('primitives', () => {
    [true, false, 0, 42, -88.8, '', 'hello world'].forEach(v => {
      assert.isTrue(equals(jsonToNoms(v), v));
    });
  });

  test('list', () => {
    assert.isTrue(equals(new List([true]), jsonToNoms([true])));
    assert.isTrue(equals(new List([true, 42]), jsonToNoms([true, 42])));
    assert.isTrue(equals(new List([new List([88.8])]), jsonToNoms([[88.8]])));
  });

  test('object', () => {
    const tests: any = [
      {},
      newStruct('', {}),
      {
        bool: true,
        num: 42,
        string: 'monkey',
        list: [],
        struct: {key: 'val'},
      },
      newStruct('', {
        bool: true,
        num: 42,
        string: 'monkey',
        list: new List([]),
        struct: newStruct('', {
          key: 'val',
        }),
      }),
      {
        _content: 42,
      },
      newStruct('', {
        Q5Fcontent: 42,
      }),
    ];

    for (let i = 0; i < tests.length; i += 2) {
      const [input, expected] = tests.slice(i, i + 2);
      const actual = jsonToNoms(input);
      assert.isTrue(equals(actual, expected));
    }
  });
});
