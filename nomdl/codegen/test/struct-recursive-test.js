// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {Tree, typeForTree} from './gen/struct_recursive.noms.js';
import {newList, makeListType} from '@attic/noms';

suite('struct_recursive.noms', () => {
  test('constructor', async () => {
    const listOfTreeType = makeListType(typeForTree);
    const t: Tree = new Tree({children: await newList([
      new Tree({children: await newList([], listOfTreeType)}),
      new Tree({children: await newList([], listOfTreeType)}),
    ], listOfTreeType)});
    assert.equal(t.children.length, 2);
  });
});
