// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {notNull} from './assert.js';
import NodeOrderedPutCache from './put-cache.js';
import BrowserOrderedPutCache from './browser/put-cache.js';
import Chunk from './chunk.js';

suite('OrderedPutCache', () => {
  for (const OrderedPutCache of [NodeOrderedPutCache, BrowserOrderedPutCache]) {
    test('append', async () => {
      const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
      const cache = new OrderedPutCache();
      assert.isTrue(cache.append(canned[0]));
      assert.isTrue(cache.append(canned[1]));
      await cache.destroy();
    });

    test('repeated append returns false', async () => {
      const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
      const cache = new OrderedPutCache();
      assert.isTrue(cache.append(canned[0]));
      assert.isTrue(cache.append(canned[1]));
      assert.isFalse(cache.append(canned[0]));
      await cache.destroy();
    });

    test('get', async () => {
      const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
      const cache = new OrderedPutCache();
      assert.isTrue(cache.append(canned[0]));

      let p = cache.get(canned[1].hash.toString());
      assert.isNull(p);

      assert.isTrue(cache.append(canned[1]));
      p = cache.get(canned[1].hash.toString());
      assert.isNotNull(p);
      const chunk = await notNull(p);
      assert.isTrue(canned[1].hash.equals(chunk.hash));

      await cache.destroy();
    });

    test('dropUntil', async () => {
      const canned = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi')];
      const cache = new OrderedPutCache();
      for (const chunk of canned) {
        assert.isTrue(cache.append(chunk));
      }

      await cache.dropUntil(canned[1].hash.toString());

      let p = cache.get(canned[2].hash.toString());
      assert.isNotNull(p);
      const chunk = await notNull(p);
      assert.isTrue(canned[2].hash.equals(chunk.hash));

      p = cache.get(canned[0].hash.toString());
      assert.isNull(p);
      p = cache.get(canned[1].hash.toString());
      assert.isNull(p);

      await cache.destroy();
    });

    test('extractChunks', async () => {
      const canned = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi')];
      const cache = new OrderedPutCache();
      for (const chunk of canned) {
        assert.isTrue(cache.append(chunk));
      }

      const chunkStream = await cache.extractChunks(canned[0].hash.toString(),
        canned[2].hash.toString());
      const chunks = [];
      await chunkStream(chunk => { chunks.push(chunk); });

      for (let i = 0; i < canned.length; i++) {
        assert.isTrue(canned[i].hash.equals(chunks[i].hash));
      }

      await cache.destroy();
    });
  }
});
