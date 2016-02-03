// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {int64} from './primitives.js';
import {SequenceChunker} from './sequence_chunker.js';

class ModBoundaryChecker {
  mod: number;

  constructor(mod: number) {
    this.mod = mod;
  }

  get windowSize(): number {
    return 1;
  }

  write(item: int64): boolean {
    return item % this.mod === 0;
  }
}

function sumChunker(items: Array<int64>): [int64, any] {
  let sum = 0;
  for (let i = 0; i < items.length; i++) {
    sum += items[i];
  }

  return [sum, items];
}

suite('SequenceChunker', () => {

  async function testChunking(expect: Array<int64>, from: number, to: number) {
    const seq = new SequenceChunker(null, sumChunker, sumChunker,
      new ModBoundaryChecker(3), () => new ModBoundaryChecker(5));

    for (let i = from; i <= to; i++) {
      seq.append(i);
    }

    assert.deepEqual(expect, await seq.done());
  }

  test('mod', async () => {
    await testChunking([1], 1, 1);
    await testChunking([3], 3, 3);
    await testChunking([1, 2], 1, 2);
    await testChunking([3, 4], 3, 4); // XX
    await testChunking([1, 2, 3], 1, 3);
    await testChunking([6, 4], 1, 4);
    await testChunking([6, 15], 1, 6);
    await testChunking([21, 7], 1, 7);
  });
});
