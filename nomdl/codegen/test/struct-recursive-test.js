// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {Tree, typeForTree, newListOfTree} from './gen/struct_recursive.noms.js';
import {newList, makeListType} from '@attic/noms';

suite('struct_recursive.noms', () => {
  test('constructor', async () => {
    const listOfTreeType = makeListType(typeForTree);
    const t: Tree = new Tree({children: await newList([
      new Tree({children: await newList([], listOfTreeType)}),
      new Tree({children: await newList([], listOfTreeType)}),
    ], listOfTreeType)});
    assert.equal(t.children.length, 2);

    const t2: Tree = new Tree({children: await newListOfTree([
      new Tree({children: await newListOfTree([])}),
      new Tree({children: await newListOfTree([])}),
    ])});
    assert.isTrue(t.equals(t2));
  });
});
