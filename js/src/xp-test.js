// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import type {valueOrPrimitive} from './value.js';
import {default as Database} from './database.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';

export class TestValue {
  _value: valueOrPrimitive;
  _expectedRef: string;
  _description: string;

  constructor(value: valueOrPrimitive, expectedRef: string, description: string) {
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
    const v2 = await db.readValue(r.targetRef);
    assert.strictEqual(v2, t._value, t._description);
    assert.strictEqual(t._value, v2, t._description);
    assert.strictEqual(t._expectedRef, r.targetRef.toString(), t._description);
  }

  async function testTypes(testValues: Array<TestValue>): Promise<void> {
    for (let i = 0; i < testValues.length; i++) {
      await roundTripDigestTest(testValues[i]);
    }
  }

  async function testSuite(): Promise<void> {
    // please update Go and JS to keep them in sync - see types/xp_test.go
    const testValues = [
      new TestValue(true, 'sha1-b6c4dd02a2f17ae9693627f03f642b988b4d5b63', 'bool - true'),
      new TestValue(false, 'sha1-dd1259720743f53a411788282c556662db14c758', 'bool - false'),
      new TestValue(-1, 'sha1-4cff7171b2664044dc02d304e8aba7fc733681a0', 'num - -1'),
      new TestValue(0, 'sha1-99b6938ab3aa497b1392fdbcb34b63bf4fe75c3c', 'num - 0'),
      new TestValue(1, 'sha1-fef7b450ff9b1e5a34dbfa9702bb78ebff1c2730', 'num - 1'),
      new TestValue('', 'sha1-9f4895d88ceab0d09962d84f6d5a93d3451ae9a3', 'str - empty'),
      new TestValue('0', 'sha1-e557fdd1c0b2661daac19b40446ffd4bafde793a', 'str - 0'),
      new TestValue('false', 'sha1-9fe813b27cf8ae1ca5d258c5299caa4f749e86c4', 'str - false'),
    ];

    await testTypes(testValues);
  }

  test('cross platform test', async () => {
    await testSuite();
  });
});


