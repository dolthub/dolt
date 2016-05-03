// @flow

import {blobType, refOfBlobType} from './type.js';
import {assert} from 'chai';
import {newBlob, BlobWriter, NomsBlob} from './blob.js';
import {suite, test} from 'mocha';
import {
  assertChunkCountAndType,
  assertValueRef,
  assertValueType,
  chunkDiffCount,
  testRoundTripAndValidate,
} from './test-util.js';
import {invariant} from './assert.js';

// IMPORTANT: These tests and in particular the hash of the values should stay in sync with the
// corresponding tests in go

suite('Blob', () => {

  async function assertReadFull(expect: Uint8Array, blob: NomsBlob): Promise<void> {
    const length = expect.length;
    const reader = blob.getReader();
    let i = 0;

    while (i < length) {
      const next = await reader.read();
      assert.isFalse(next.done);
      const arr = next.value;
      invariant(arr);
      for (let j = 0; j < arr.length; j++) {
        assert.strictEqual(expect[i], arr[j]);
        i++;
      }
    }
  }

  async function testPrependChunkDiff(buff: Uint8Array, blob: NomsBlob, expectCount: number):
      Promise<void> {
    const nb = new Uint8Array(buff.length + 1);
    for (let i = 0; i < buff.length; i++) {
      nb[i + 1] = buff[i];
    }

    const v2 = await newBlob(nb);
    assert.strictEqual(expectCount, chunkDiffCount(blob, v2));
  }

  async function testAppendChunkDiff(buff: Uint8Array, blob: NomsBlob, expectCount: number):
      Promise<void> {
    const nb = new Uint8Array(buff.length + 1);
    for (let i = 0; i < buff.length; i++) {
      nb[i] = buff[i];
    }

    const v2 = await newBlob(nb);
    assert.strictEqual(expectCount, chunkDiffCount(blob, v2));
  }

  function randomBuff(len: number): Uint8Array {
    const r = new CountingByteReader();
    const a = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      a[i] = r.nextUint8();
    }
    return a;
  }

  async function blobTestSuite(size: number, expectRefStr: string, expectChunkCount: number,
                               expectPrependChunkDiff: number,
                               expectAppendChunkDiff: number) {
    const length = 1 << size;
    const buff = randomBuff(length);
    const blob = await newBlob(buff);

    assertValueRef(expectRefStr, blob);
    assertValueType(blobType, blob);
    assert.strictEqual(length, blob.length);
    assertChunkCountAndType(expectChunkCount, refOfBlobType, blob);

    await testRoundTripAndValidate(blob, async(b2) => {
      await assertReadFull(buff, b2);
    });

    // TODO: Random Read

    await testPrependChunkDiff(buff, blob, expectPrependChunkDiff);
    await testAppendChunkDiff(buff, blob, expectAppendChunkDiff);
  }

  class CountingByteReader {
    _z: number;
    _value: number;
    _count: number;

    constructor(seed: number = 0) {
      this._z = seed;
      this._value = seed;
      this._count = 4;
    }

    nextUint8(): number {
      // Increment number
      if (this._count === 0) {
        this._z++;
        this._value = this._z;
        this._count = 4;
      }

      // Unshift a uint8 from our current number
      const retval = this._value & 0xff;
      this._value = this._value >>> 8;
      this._count--;

      return retval;
    }
  }

  test('Blob 1K', async () => {
    await blobTestSuite(10, 'sha1-cb21e6231cbcf57ff8a9e80c9cbc5b1e798bf9ea', 3, 2, 2);
  });

  test('LONG: Blob 4K', async () => {
    await blobTestSuite(12, 'sha1-53344f6e1d41ed9ce781e6cb3b999d3c5fc242a4', 9, 2, 2);
  });

  test('LONG: Blob 16K', async () => {
    await blobTestSuite(14, 'sha1-50821a3ce89449bbc490194000c380e979e79132', 33, 2, 2);
  });

  test('LONG: Blob 64K', async () => {
    await blobTestSuite(16, 'sha1-097098adc9ad9663c30ffcf69933b44165df1226', 4, 2, 2);
  });

  test('LONG: Blob 256K', async () => {
    await blobTestSuite(18, 'sha1-ff9c888571d2317bd201cfc2d31dbfd6a546c629', 13, 2, 2);
  });

  test('BlobWriter', async () => {
    const a = randomBuff(15);
    const b1 = await newBlob(a);
    const w = new BlobWriter();
    w.write(new Uint8Array(a.buffer, 0, 5));
    w.write(new Uint8Array(a.buffer, 5, 5));
    w.write(new Uint8Array(a.buffer, 10, 5));
    await w.close();
    const b2 = w.blob;
    const b3 = w.blob;
    assert.strictEqual(b2, b3);
    assert.isTrue(b1.equals(b2));
  });

  test('BlobWriter blob throws', async () => {
    const a = randomBuff(15);
    const w = new BlobWriter();
    w.write(a);
    w.close();  // No await, so not closed
    let ex;
    try {
      w.blob;
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);

    try {
      await w.close();  // Cannot close twice.
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);
  });

  test('BlobWriter close throws', async () => {
    const a = randomBuff(15);
    const w = new BlobWriter();
    w.write(a);
    w.close();  // No await, so closing

    let ex;
    try {
      await w.close();  // Cannot close twice.
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);
  });

  test('BlobWriter write throws', async () => {
    const a = randomBuff(15);
    const w = new BlobWriter();
    w.write(a);
    await w.close();  // No await, so closing

    let ex;
    try {
      w.write(a);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);
  });
});
