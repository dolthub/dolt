// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import type Value from './value.js';
import {TestDatabase} from './test-util.js';
import {equals} from './compare.js';

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
    const db = new TestDatabase();
    const r = db.writeValue(t._value);
    const v2 = await db.readValue(r.targetHash);
    assert.isTrue(equals(v2, t._value), t._description);
    assert.isTrue(equals(t._value, v2), t._description);
    assert.strictEqual(v2, t._value, t._description);
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
      new TestValue(true, 'g19moobgrm32dn083bokhksuobulq28c', 'bool - true'),
      new TestValue(false, 'bqjhrhmgmjqnnssqln87o84c6no6pklq', 'bool - false'),
      new TestValue(-1, 'hq0jvv1enraehfggfk8s27ll1rmirt96', 'num - -1'),
      new TestValue(0, 'elie88b5iouak7onvi2mpkcgoqqr771l', 'num - 0'),
      new TestValue(1, '6h9ldndhjoq0r5sbn1955gaearq5dovc', 'num - 1'),
      new TestValue(-122.411912027329, 'hcdjnev3lccjplue6pb0fkhgeehv6oec',
        'num - -122.411912027329'),
      new TestValue(Number.MAX_SAFE_INTEGER, '3fpnjghte4v4q8qogl4bga0qldetlo7b',
        'num - 9007199254740991'),
      new TestValue(Number.MIN_SAFE_INTEGER, 'jd80frddd2fs3q567tledcgmfs85dvke',
        'num - -9007199254740991'),
      new TestValue(Number.EPSILON, 'qapetp8502l672v2vie52nd4qjviq5je',
        'num - 2.220446049250313e-16'),
      // Go math.MaxFloat64
      new TestValue(1.7976931348623157e+308, '9bqr7ofsvhutqo5ue1iqpmsu70e85ll6',
        'num - 1.7976931348623157e+308'),
      new TestValue('', 'ssfs0o2eq3kg50p37q2crhhqhjcs2391', 'str - empty'),
      new TestValue('0', 'jngc7d11d2h0c6s2f15l10rckvu753rb', 'str - 0'),
      new TestValue('false', '1v3a1t4to25kkohm1bhh2thebmls0lp0', 'str - false'),
    ];

    await testTypes(testValues);
  }

  test('cross platform test', async () => {
    await testSuite();
  });
});
