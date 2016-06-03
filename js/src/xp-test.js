// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';
import type Value from './value.js';
import Database from './database.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';

export class TestValue {
  _value: Value;
  _expectedRef: string;
  _description: string;

  constructor(value: Value, expectedRef: string, description: string) {
    this._value = value;
    this._expectedRef = expectedRef;
    this._description = description;
  }
}

suite('cross platform test', () => {
  // write a value, read that value back out
  // assert the values are equal and
  // verify the digest is what we expect
  async function roundTripDigestTest(t: TestValue): Promise<void> {
    const db = new Database(makeTestingBatchStore());
    const r = db.writeValue(t._value);
    const v2 = await db.readValue(r.targetHash);
    assert.strictEqual(v2, t._value, t._description);
    assert.strictEqual(t._value, v2, t._description);
    assert.strictEqual(t._expectedRef, r.targetHash.toString(), t._description);
    return db.close();
  }

  async function testTypes(testValues: Array<TestValue>): Promise<void> {
    for (let i = 0; i < testValues.length; i++) {
      await roundTripDigestTest(testValues[i]);
    }
  }

  async function testSuite(): Promise<void> {
    // please update Go and JS to keep them in sync - see types/xp_test.go
    const testValues = [
      new TestValue(true, 'sha1-3f29546453678b855931c174a97d6c0894b8f546', 'bool - true'),
      new TestValue(false, 'sha1-1489f923c4dca729178b3e3233458550d8dddf29', 'bool - false'),
      new TestValue(-1, 'sha1-cd243416f913f4a81d020a866266316b30200e34', 'num - -1'),
      new TestValue(0, 'sha1-80e331473af6cb0cd7ae6f75793070cfbc4d642b', 'num - 0'),
      new TestValue(1, 'sha1-9f34f68652a49c4b7cc5e25951311e92c61d46d0', 'num - 1'),
      new TestValue('', 'sha1-e1bc1dae59f116abb43f9dafbb2acc9b141aa6b0', 'str - empty'),
      new TestValue('0', 'sha1-a1c90c71d1ffdb51138677c578e6f2e8a011070d', 'str - 0'),
      new TestValue('false', 'sha1-e15d53dc6c9d3aa6eca4eea28382c9c45ba8fd9e', 'str - false'),
    ];

    await testTypes(testValues);
  }

  test('cross platform test', async () => {
    await testSuite();
  });
});
