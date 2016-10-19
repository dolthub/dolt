// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {equals} from './compare.js';

import {getHash} from './get-hash.js';
import {notNull} from './assert.js';
import List from './list.js';
import Map from './map.js';
import Path from './path.js';
import Ref from './ref.js';
import Set from './set.js';
import type Value from './value.js';
import {newStruct} from './struct.js';

function hashIdx(v: Value): string {
  return `[#${getHash(v).toString()}]`;
}

async function assertResolvesTo(expect: Value | null, ref: Value, str: string) {
  const j = s => JSON.stringify(s);
  const p = Path.parse(str);
  const actual = await p.resolve(ref);
  if (expect === null) {
    assert.isTrue(actual === null, `Expected null, but got ${j(actual)}`);
  } else if (actual === null) {
    assert.isTrue(false, `Expected ${j(expect)}, but got null`);
  } else {
    assert.isTrue(equals(notNull(expect), actual), `Expected ${j(expect)}, but got ${j(actual)}`);
  }
}

suite('Path', () => {
  test('struct', async () => {
    const v = newStruct('', {
      foo: 'foo',
      bar: false,
      baz: 203,
    });

    await assertResolvesTo('foo', v, '.foo');
    await assertResolvesTo(false, v, '.bar');
    await assertResolvesTo(203, v, '.baz');
    await assertResolvesTo(null, v, '.notHere');

    const v2 = newStruct('', {
      v1: v,
    });

    await assertResolvesTo('foo', v2, '.v1.foo');
    await assertResolvesTo(false, v2, '.v1.bar');
    await assertResolvesTo(203, v2, '.v1.baz');
    await assertResolvesTo(null, v2, '.v1.notHere');
    await assertResolvesTo(null, v2, '.notHere.foo');
  });

  test('index', async () => {
    let v: Value;
    const resolvesTo = async (exp: Value | null, val: Value, str: string) => {
      // Indices resolve to |exp|.
      await assertResolvesTo(exp, v, str);
      // Keys resolves to themselves.
      if (exp !== null) {
        exp = val;
      }
      await assertResolvesTo(exp, v, str + '@key');
    };

    v = new List([1, 3, 'foo', false]);

    await resolvesTo(1, 0, '[0]');
    await resolvesTo(3, 1, '[1]');
    await resolvesTo('foo', 2, '[2]');
    await resolvesTo(false, 3, '[3]');
    await resolvesTo(null, 4, '[4]');
    await resolvesTo(null, -4, '[-4]');

    v = new Map([
      [1, 'foo'],
      ['two', 'bar'],
      [false, 23],
      [2.3, 4.5],
    ]);

    await resolvesTo('foo', 1, '[1]');
    await resolvesTo('bar', 'two', '["two"]');
    await resolvesTo(23, false, '[false]');
    await resolvesTo(4.5, 2.3, '[2.3]');
    await resolvesTo(null, 4, '[4]');
  });

  test('hash index', async () => {
    const b = true;
    const br = new Ref(b);
    const i = 0;
    const str = 'foo';
    const l = new List([b, i, str]);
    const lr = new Ref(l);
    const m = new Map([
      [b, br],
      [br, i],
      [i, str],
      [l, lr],
      [lr, b],
    ]);
    const s = new Set([b, br, i, str, l, lr]);

    const resolvesTo = async (col: Value, exp: Value | null, val: Value) => {
      // Values resolve to |exp|.
      await assertResolvesTo(exp, col, hashIdx(val));
      // Keys resolves to themselves.
      if (exp !== null) {
        exp = val;
      }
      await assertResolvesTo(exp, col, hashIdx(val) + '@key');
    };

    // Primitives are only addressable by their values.
    await resolvesTo(m, null, b);
    await resolvesTo(m, null, i);
    await resolvesTo(m, null, str);
    await resolvesTo(s, null, b);
    await resolvesTo(s, null, i);
    await resolvesTo(s, null, str);

    // Other values are only addressable by their hashes.
    await resolvesTo(m, i, br);
    await resolvesTo(m, lr, l);
    await resolvesTo(m, b, lr);
    await resolvesTo(s, br, br);
    await resolvesTo(s, l, l);
    await resolvesTo(s, lr, lr);

    // Lists cannot be addressed by hashes, obviously.
    await resolvesTo(l, null, i);
  });

  test('hash index of singleton collection', async () => {
    // This test is to make sure we don't accidentally return the element of a singleton map.
    const resolvesToNull = async (col: Value, v: Value) => {
      await assertResolvesTo(null, col, hashIdx(v));
    };

    await resolvesToNull(new Map([[true, true]]), true);
    await resolvesToNull(new Set([true]), true);
  });

  test('multi', async () => {
    const m1 = new Map([
      ['a', 'foo'],
      ['b', 'bar'],
      ['c', 'car'],
    ]);

    const m2 = new Map([
      ['d', 'dar'],
      [false, 'earth'],
      [m1, 'fire'],
    ]);

    const l = new List([m1, m2]);

    const s = newStruct('', {
      'foo': l,
    });

    await assertResolvesTo(l, s, '.foo');
    await assertResolvesTo(m1, s, '.foo[0]');
    await assertResolvesTo('foo', s, '.foo[0]["a"]');
    await assertResolvesTo('bar', s, '.foo[0]["b"]');
    await assertResolvesTo('car', s, '.foo[0]["c"]');
    await assertResolvesTo(null, s, '.foo[0]["x"]');
    await assertResolvesTo(null, s, '.foo[2]["c"]');
    await assertResolvesTo(null, s, '.notHere[0]["c"]');
    await assertResolvesTo(m2, s, '.foo[1]');
    await assertResolvesTo('dar', s, '.foo[1]["d"]');
    await assertResolvesTo('earth', s, '.foo[1][false]');
    await assertResolvesTo('fire', s, `.foo[1]${hashIdx(m1)}`);
    await assertResolvesTo(m1, s, `.foo[1]${hashIdx(m1)}@key`);
    await assertResolvesTo('car', s, `.foo[1]${hashIdx(m1)}@key["c"]`);
  });

  test('parse success', () => {
    const t = (s: string) => {
      const p = Path.parse(s);
      let expect = s;
      // Human readable serialization special cases.
      if (expect === '[1e4]') {
        expect = '[10000]';
      } else if (expect === '[1.]') {
        expect = '[1]';
      } else if (expect === '["line\nbreak\rreturn"]') {
        expect = '["line\\nbreak\\rreturn"]';
      }
      assert.strictEqual(expect, p.toString());
    };

    const h = getHash(42); // arbitrary hash

    t('.foo');
    t('.Q');
    t('.QQ');
    t('[true]');
    t('[false]');
    t('[false]@key');
    t('[42]');
    t('[42]@key');
    t('[1e4]');
    t('[1.]');
    t('[1.345]');
    t('[""]');
    t('["42"]');
    t('["42"]@key');
    t('[\"line\nbreak\rreturn\"]');
    t('["qu\\\\ote\\\""]');
    t('["π"]');
    t('["[[br][]acke]]ts"]');
    t('["xπy✌z"]');
    t('["ಠ_ಠ"]');
    t('["0"]["1"]["100"]');
    t('.foo[0].bar[4.5][false]');
    t(`.foo[#${h.toString()}]`);
    t(`.bar[#${h.toString()}]@key`);
  });

  test('parse errors', () => {
    const t = (s: string, expectErr: string) => {
      let actualErr = '';
      try {
        Path.parse(s);
      } catch (e) {
        assert.instanceOf(e, SyntaxError);
        actualErr = e.message;
      }
      assert.strictEqual(expectErr, actualErr);
    };

    t('', 'Empty path');
    t('.', 'Invalid field: ');
    t('[', 'Path ends in [');
    t(']', '] is missing opening [');
    t('.#', 'Invalid field: #');
    t('. ', 'Invalid field:  ');
    t('. invalid.field', 'Invalid field:  invalid.field');
    t('.foo.', 'Invalid field: ');
    t('.foo.#invalid.field', 'Invalid field: #invalid.field');
    t('.foo!', 'Invalid operator: !');
    t('.foo!bar', 'Invalid operator: !');
    t('.foo#', 'Invalid operator: #');
    t('.foo#bar', 'Invalid operator: #');
    t('.foo[', 'Path ends in [');
    t('.foo[.bar', '[ is missing closing ]');
    t('.foo]', '] is missing opening [');
    t('.foo].bar', '] is missing opening [');
    t('.foo[]', 'Empty index value');
    t('.foo[[]', 'Invalid index: [');
    t('.foo[[]]', 'Invalid index: [');
    t('.foo[42.1.2]', 'Invalid index: 42.1.2');
    t('.foo[1f4]', 'Invalid index: 1f4');
    t('.foo[hello]', 'Invalid index: hello');
    t('.foo[\'hello\']', 'Invalid index: \'hello\'');
    t('.foo[\\]', 'Invalid index: \\');
    t('.foo[\\\\]', 'Invalid index: \\\\');
    t('.foo["hello]', '[ is missing closing ]');
    t('.foo["hello', '[ is missing closing ]');
    t('.foo["', '[ is missing closing ]');
    t('.foo["\\', '[ is missing closing ]');
    t('.foo["]', '[ is missing closing ]');
    t('.foo[#]', 'Invalid hash: ');
    t('.foo[#invalid]', 'Invalid hash: invalid');
    t('.foo["hello\\nworld"]', 'Only " and \\ can be escaped');
    t('.foo[42]bar', 'Invalid operator: b');
    t('#foo', 'Invalid operator: #');
    t('!foo', 'Invalid operator: !');
    t('@foo', 'Invalid operator: @');
    t('@key', 'Invalid operator: @');
    t('.foo[42]@soup', 'Unsupported annotation: @soup');
  });
});
