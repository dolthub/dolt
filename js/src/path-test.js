// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {equals} from './compare.js';

import Path from './path.js';
import type {valueOrPrimitive} from './value.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newStruct} from './struct.js';

suite('Path', () => {

  async function assertPathEqual(expect: any, ref: valueOrPrimitive, path: Path):
      Promise<void> {
    // $FlowIssue: need to be able to pass in null for ref
    const actual = await path.resolve(ref);
    if (actual === undefined || expect === undefined) {
      assert.strictEqual(expect, actual);
      return;
    }

    assert.isTrue(equals(expect, actual));
  }

  test('struct', async () => {
    const v = newStruct('', {
      foo: 'foo',
      bar: false,
      baz: 203,
    });

    await assertPathEqual('foo', v, new Path().addField('foo'));
    await assertPathEqual(false, v, new Path().addField('bar'));
    await assertPathEqual(203, v, new Path().addField('baz'));
    await assertPathEqual(undefined, v, new Path().addField('notHere'));

    const v2 = newStruct('', {
      v1: v,
    });

    await assertPathEqual('foo', v2, new Path().addField('v1').addField('foo'));
    await assertPathEqual(false, v2, new Path().addField('v1').addField('bar'));
    await assertPathEqual(203, v2, new Path().addField('v1').addField('baz'));
    await assertPathEqual(undefined, v2, new Path().addField('v1').addField('notHere'));
    await assertPathEqual(undefined, v2, new Path().addField('notHere').addField('foo'));
  });

  test('list', async () => {
    const v = await newList([1, 3, 'foo', false]);

    await assertPathEqual(1, v, new Path().addIndex(0));
    await assertPathEqual(3, v, new Path().addIndex(1));
    await assertPathEqual('foo', v, new Path().addIndex(2));
    await assertPathEqual(false, v, new Path().addIndex(3));
    await assertPathEqual(undefined, v, new Path().addIndex(4));
    await assertPathEqual(undefined, v, new Path().addIndex(-4));
  });

  test('map', async () => {
    const v = await newMap([[1, 'foo'], ['two', 'bar'], [false, 23], [2.3, 4.5]]);

    await assertPathEqual('foo', v, new Path().addIndex(1));
    await assertPathEqual('bar', v, new Path().addIndex('two'));
    await assertPathEqual(23, v, new Path().addIndex(false));
    await assertPathEqual(4.5, v, new Path().addIndex(2.3));
    await assertPathEqual(undefined, v, new Path().addIndex(4));
  });

  test('struct.list.map', async () => {
    const m1 = await newMap([['a', 'foo'], ['b','bar'], ['c', 'car']]);
    const m2 = await newMap([['d', 'dar'], [false, 'earth']]);
    const l = await newList([m1, m2]);
    const s = newStruct('', {
      foo: l,
    });

    await assertPathEqual(l, s, new Path().addField('foo'));
    await assertPathEqual(m1, s, new Path().addField('foo').addIndex(0));
    await assertPathEqual('foo', s, new Path().addField('foo').addIndex(0).addIndex('a'));
    await assertPathEqual('bar', s, new Path().addField('foo').addIndex(0).addIndex('b'));
    await assertPathEqual('car', s, new Path().addField('foo').addIndex(0).addIndex('c'));
    await assertPathEqual(undefined, s, new Path().addField('foo').addIndex(0).addIndex('x'));
    await assertPathEqual(undefined, s, new Path().addField('foo').addIndex(2).addIndex('c'));
    await assertPathEqual(undefined, s, new Path().addField('notHere').addIndex(2).addIndex('c'));
    await assertPathEqual(m2, s, new Path().addField('foo').addIndex(1));
    await assertPathEqual('dar', s, new Path().addField('foo').addIndex(1).addIndex('d'));
    await assertPathEqual('earth', s, new Path().addField('foo').addIndex(1).addIndex(false));
  });

  function assertToString(expect: string, path: Path) {
    assert.strictEqual(expect, path.toString());
  }

  test('toString()', () => {
    assertToString('[0][1][100]', new Path().addIndex(0).addIndex(1).addIndex(100));
    assertToString('["0"]["1"]["100"]',
        new Path().addIndex('0').addIndex('1').addIndex('100'));
    assertToString('.foo[0].bar[4.5][false]',
        new Path().addField('foo').addIndex(0).addField('bar').addIndex(4.5).addIndex(false));
  });
});
