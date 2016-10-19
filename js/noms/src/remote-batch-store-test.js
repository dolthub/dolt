// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import makeRemoteBatchStoreFake from './remote-batch-store-fake.js';
import {encodeValue} from './codec.js';

suite('BatchStore', () => {
  test('get after schedulePut works immediately', async () => {
    const bs = makeRemoteBatchStoreFake();
    const input = 'abc';

    const c = encodeValue(input);
    bs.schedulePut(c, new Set());

    const chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));
    await bs.close();
  });

  test('get after schedulePut works after flush', async () => {
    const bs = makeRemoteBatchStoreFake();
    const input = 'abc';

    const c = encodeValue(input);
    bs.schedulePut(c, new Set());

    let chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));

    await bs.flush();
    chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));
    await bs.close();
  });
});
