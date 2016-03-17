// @flow

import {assert} from 'chai';
import {suite} from 'mocha';
import Random from './pseudo-random.js';
import MemoryStore from './memory-store.js';
import test from './async-test.js';
import {blobType} from './type.js';
import {readValue} from './read-value.js';
import {writeValue} from './encode.js';
import {newBlob, BlobWriter} from './blob.js';

suite('Blob', () => {
  function intSequence(start: number, end: number): Uint8Array {
    const nums = new Uint8Array(end - start);

    for (let i = start; i < end; i++) {
      nums[i - start] = i & 0xff;
    }

    return nums;
  }

  function firstNNumbers(n: number): Uint8Array {
    return intSequence(0, n);
  }

  function randomArray(len: number): Uint8Array {
    const r = new Random(102);  // Picked so we get a chunk.
    const a = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      a[i] = r.nextUint8();
    }
    return a;
  }

  test('length', async () => {
    const b1 = await newBlob(new Uint8Array([0]));
    assert.equal(b1.length, 1);

    const b2 = await newBlob(firstNNumbers(256));
    assert.equal(b2.length, 256);
  });

  test('equals', async () => {
    const b1 = await newBlob(firstNNumbers(10));
    const b2 = await newBlob(firstNNumbers(10));
    assert.isTrue(b1.equals(b2));
    assert.isTrue(b2.equals(b1));
  });

  test('roundtrip', async () => {
    const ms = new MemoryStore();

    const b1 = await newBlob(randomArray(15));
    const r1 = await writeValue(b1, blobType, ms);
    const b2 = await readValue(r1, ms);
    assert.isTrue(b1.equals(b2));
  });

  test('BlobWriter', async () => {
    const a = randomArray(15);
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
    const a = randomArray(15);
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
    const a = randomArray(15);
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
    const a = randomArray(15);
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

  test('same as in Go', async () => {
    const b = await newBlob(new Uint8Array([
      141,
      136,
      71,
      250,
      17,
      60,
      107,
      206,
      213,
      48,
      207,
      226,
      217,
      100,
      115,
    ]));
    assert.equal(b.ref.toString(), 'sha1-fc30f237649464078574bc46b90c842179b4fa18');
  });
});
