// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {equals} from './compare.js';

import {invariant, notNull} from './assert.js';
import AbsolutePath from './absolute-path.js';
import {getHash} from './get-hash.js';
import {stringLength} from './hash.js';
import List from './list.js';
import Set from './set.js';
import {TestDatabase} from './test-util.js';
import type Value from './value.js';

suite('AbsolutePath', () => {
  test('to and from string', () => {
    const t = (str: string) => {
      const p = AbsolutePath.parse(str);
      assert.strictEqual(str, p.toString());
    };

    const h = getHash(42); // arbitrary hash
    t(`foo.bar[#${h.toString()}]`);
    t(`#${h.toString()}.bar[42]`);
  });

  test('absolute paths', async () => {
    const s0 = 'foo', s1 = 'bar';
    const list = new List([s0, s1]);
    const emptySet = new Set();

    const db = new TestDatabase();
    db.writeValue(s0);
    db.writeValue(s1);
    db.writeValue(list);
    db.writeValue(emptySet);

    let ds = db.getDataset('ds');
    ds = await db.commit(ds, list);
    const head = await ds.head();
    invariant(head);

    const resolvesTo = async (exp: Value | null, str: string) => {
      const p = AbsolutePath.parse(str);
      const act = await notNull(p).resolve(db);
      if (exp === null) {
        assert.strictEqual(null, act);
      } else if (act === null) {
        assert.isTrue(false, `Failed to resolve ${str}`);
      } else {
        assert.isTrue(equals(exp, act));
      }
    };

    await resolvesTo(head, 'ds');
    await resolvesTo(emptySet, 'ds.parents');
    await resolvesTo(list, 'ds.value');
    await resolvesTo(s0, 'ds.value[0]');
    await resolvesTo(s1, 'ds.value[1]');
    await resolvesTo(head, '#' + getHash(head).toString());
    await resolvesTo(list, '#' + getHash(list).toString());
    await resolvesTo(s0, `#${getHash(s0).toString()}`);
    await resolvesTo(s1, `#${getHash(s1).toString()}`);
    await resolvesTo(s0, `#${getHash(list).toString()}[0]`);
    await resolvesTo(s1, `#${getHash(list).toString()}[1]`);

    await resolvesTo(null, 'foo');
    await resolvesTo(null, 'foo.parents');
    await resolvesTo(null, 'foo.value');
    await resolvesTo(null, 'foo.value[0]');
    await resolvesTo(null, `#${getHash('baz').toString()}`);
    await resolvesTo(null, `#${getHash('baz').toString()}[0]`);
  });

  test('parse errors', () => {
    const t = (path: string, exp: string) => {
      let act = '';
      try {
        AbsolutePath.parse(path);
      } catch (e) {
        assert.instanceOf(e, SyntaxError);
        act = e.message;
      }
      assert.strictEqual(exp, act);
    };

    t('', 'Empty path');
    t('.foo', 'Invalid dataset name: .foo');
    t('.foo.bar.baz', 'Invalid dataset name: .foo.bar.baz');
    t('#', 'Invalid hash: ');
    t('#abc', 'Invalid hash: abc');
    const invalidHash = new Array(stringLength).join('z');
    t(`#${invalidHash}`, `Invalid hash: ${invalidHash}`);
  });
});
