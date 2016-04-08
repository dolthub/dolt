// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {OptionalStruct} from './gen/struct_optional.noms.js';

suite('struct_optional.noms', () => {
  test('constructor', async () => {
    const os = new OptionalStruct({});
    assert.isUndefined(os.s);
    assert.isUndefined(os.b);

    const os2 = os.setS('hi');
    assert.equal(os2.s, 'hi');
    assert.isUndefined(os2.b);

    const os3 = os2.setB(true);
    assert.equal(os3.s, 'hi');
    assert.equal(os3.b, true);

    const os4 = os2.setB(undefined).setS(undefined);
    assert.isTrue(os4.equals(os));
  });
});
