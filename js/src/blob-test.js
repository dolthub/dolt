// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {blobType, refOfBlobType} from './type.js';
import {assert} from 'chai';
import Blob, {BlobReader, BlobWriter} from './blob.js';
import {suite, test, setup, teardown} from 'mocha';
import {
  assertChunkCountAndType,
  assertValueHash,
  assertValueType,
  chunkDiffCount,
  testRoundTripAndValidate,
} from './test-util.js';
import {invariant} from './assert.js';
import {equals} from './compare.js';
import {TestDatabase} from './test-util.js';

// IMPORTANT: These tests and in particular the hash of the values should stay in sync with the
// corresponding tests in go

suite('Blob', () => {

  async function assertReadFull(expect: Uint8Array, reader: BlobReader): Promise<void> {
    const length = expect.length;
    let i = 0;
    let pos = reader._pos;

    while (i < length) {
      const next = await reader.read();
      assert.isFalse(next.done);
      const arr = next.value;
      invariant(arr);
      assert.strictEqual(arr.length + pos, reader._pos);
      pos = reader._pos;
      for (let j = 0; j < arr.length && i < length; j++) {
        assert.strictEqual(expect[i], arr[j]);
        i++;
      }
    }
  }

  async function testPrependChunkDiff(buff: Uint8Array, blob: Blob, expectCount: number):
      Promise<void> {
    const nb = new Uint8Array(buff.length + 1);
    for (let i = 0; i < buff.length; i++) {
      nb[i + 1] = buff[i];
    }

    const v2 = new Blob(nb);
    assert.strictEqual(expectCount, chunkDiffCount(blob, v2));
  }

  async function testAppendChunkDiff(buff: Uint8Array, blob: Blob, expectCount: number):
      Promise<void> {
    const nb = new Uint8Array(buff.length + 1);
    for (let i = 0; i < buff.length; i++) {
      nb[i] = buff[i];
    }

    const v2 = new Blob(nb);
    assert.strictEqual(expectCount, chunkDiffCount(blob, v2));
  }

  async function testRandomRead(buff: Uint8Array, blob: Blob): Promise<void> {
    const checkByteRange = async (start: number, rel: number, count: number) => {
      const buffSlice = new Uint8Array(buff.buffer, buff.byteOffset + rel + start, count);
      const blobReader = blob.getReader();
      assert.strictEqual(start, await blobReader.seek(start));
      assert.strictEqual(start, blobReader._pos);
      assert.strictEqual(start + rel, await blobReader.seek(rel, 1));
      assert.strictEqual(start + rel, blobReader._pos);
      await assertReadFull(buffSlice, blobReader);
    };

    const checkByteRangeFromEnd = async (length: number, offset: number, count: number) => {
      const buffSlice = new Uint8Array(buff.buffer,
                                       buff.byteOffset + buff.byteLength + offset,
                                       count);
      const blobReader = blob.getReader();
      assert.strictEqual(length + offset, await blobReader.seek(offset, 2));
      assert.strictEqual(length + offset, blobReader._pos);
      await assertReadFull(buffSlice, blobReader);
    };

    const length = buff.byteLength;
    let start = 0;
    let count = length / 2;
    while (count > 2) {
      await checkByteRange(start, 0, count);
      await checkByteRange(0, start, count);
      await checkByteRange(Math.floor(start / 2), Math.ceil(start / 2), count);
      await checkByteRangeFromEnd(length, start - length, count);
      start += count;
      count = (length - start) / 2;
    }
  }

  function randomBuff(len: number): Uint8Array {
    const r = new CountingByteReader();
    const a = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      a[i] = r.nextUint8();
    }
    return a;
  }

  async function blobTestSuite(size: number, expectHashStr: string, expectChunkCount: number,
                               expectPrependChunkDiff: number,
                               expectAppendChunkDiff: number) {
    const length = 1 << size;
    const buff = randomBuff(length);
    const blob = new Blob(buff);

    assertValueHash(expectHashStr, blob);
    assertValueType(blobType, blob);
    assert.strictEqual(length, blob.length);
    assertChunkCountAndType(expectChunkCount, refOfBlobType, blob);

    await testRoundTripAndValidate(blob, async(b2) => {
      await assertReadFull(buff, b2.getReader());
    });

    await testPrependChunkDiff(buff, blob, expectPrependChunkDiff);
    await testAppendChunkDiff(buff, blob, expectAppendChunkDiff);
    await testRandomRead(buff, blob);
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

  test('Blob 1K', () =>
    blobTestSuite(10, 'sha1-225cb62f282db9950802a8a0dce55b577af16e86', 3, 2, 2));

  test('LONG: Blob 4K', () =>
    blobTestSuite(12, 'sha1-5171d9ff4c8b7420a22cdec5c1282b6fbcafa0d5', 9, 2, 2));

  test('LONG: Blob 16K', () =>
    blobTestSuite(14, 'sha1-8741539c258f9c464b08d099cb2521f19138eae7', 2, 2, 2));

  test('LONG: Blob 64K', () =>
    blobTestSuite(16, 'sha1-f2563df4e20835fb3402837272a24f58e9e48bd8', 3, 2, 2));

  test('LONG: Blob 256K', () =>
    blobTestSuite(18, 'sha1-f97d8d77fb1e3ef21f2ccccbde810151b4e8c4e9', 8, 2, 2));

  suite('BlobWriter', () => {
    let db;

    setup(() => {
      db = new TestDatabase();
    });

    teardown((): Promise<void> => db.close());

    test('BlobWriter', () => {
      const a = randomBuff(15);
      const b1 = new Blob(a);
      const w = new BlobWriter();
      w.write(new Uint8Array(a.buffer, 0, 5));
      w.write(new Uint8Array(a.buffer, 5, 5));
      w.write(new Uint8Array(a.buffer, 10, 5));
      w.close();
      const b2 = w.blob;
      const b3 = w.blob;
      assert.strictEqual(b2, b3);
      assert.isTrue(equals(b1, b2));
    });

    test('BlobWriter close throws', () => {
      const a = randomBuff(15);
      const w = new BlobWriter();
      w.write(a);
      w.close();

      let ex;
      try {
        w.close();  // Cannot close twice.
      } catch (e) {
        ex = e;
      }
      assert.instanceOf(ex, TypeError);
    });

    test('BlobWriter write throws', () => {
      const a = randomBuff(15);
      const w = new BlobWriter();
      w.write(a);
      w.close();

      let ex;
      try {
        w.write(a);  // Cannot write after close.
      } catch (e) {
        ex = e;
      }
      assert.instanceOf(ex, TypeError);
    });

    test('BlobWriter with ValueReadWriter', () => {
      const a = randomBuff(1500);
      const b1 = new Blob(a);
      const w = new BlobWriter(db);

      // The number of writes depends on how many chunks we've encountered.
      let writes = 0;
      assert.equal(db.writeCount, writes);

      w.write(new Uint8Array(a.buffer, 0, 500));
      assert.equal(db.writeCount, writes);

      w.write(new Uint8Array(a.buffer, 500, 500));
      writes++;
      assert.equal(db.writeCount, writes);

      w.write(new Uint8Array(a.buffer, 1000, 500));
      writes++;
      assert.equal(db.writeCount, writes);

      w.close();
      writes += 2;  // one for the last leaf chunk and one for the meta chunk.
      assert.equal(db.writeCount, writes);

      const b2 = w.blob;
      const b3 = w.blob;
      assert.strictEqual(b2, b3);
      assert.isTrue(equals(b1, b2));
    });
  });
});
