// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {assert} from 'chai';
import BuzHash from './buzhash.js';
import * as Bytes from './bytes.js';

const loremipsum1 = `Lorem ipsum dolor sit amet, consectetuer adipiscing elit.
Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et
magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis,
ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis
enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In
enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis
eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum
semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu,
consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra
quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet.
Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur
ullamcorper ultricies nisi. Nam eget dui.

Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper
libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel,
luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt
tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam
sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit
amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum
sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce
vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed,
nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis
hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et
ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia.`;

const loremipsum2 = `Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget,
imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer
ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent
adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus.
Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis
vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet
feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in
justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque.
Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi.

Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo.
Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae
tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit.
Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia
Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac
placerat dolor lectus quis orci. Phasellus consectetuer vestibulum elit. Aenean
tellus metus, bibendum sed, posuere ac, mattis non, nunc. Vestibulum fringilla
pede sit amet augue. In turpis. Pellentesque posuere. Praesent turpis.`;

suite('BuzHash', () => {
  test('rolling hash', () => {
    function test(n: number, phrase: Uint8Array, text: Uint8Array, pos: number) {
      const hasher1 = new BuzHash(n);
      hasher1.write(phrase);
      const p1sum = hasher1.sum32;

      hasher1.reset();
      let found = false;
      for (let idx = 0; idx < text.length; idx++) {
        const b = text[idx];
        const ssum = hasher1.hashByte(b);
        if ((ssum === p1sum) && ((idx - n) === pos)) {
          found = true;
          break;
        }
      }

      assert.isTrue(found);
    }

    test(32,
      Bytes.fromString('Aenean massa. Cum sociis natoque'),
      Bytes.fromString(loremipsum1), 91);
    test(64,
      Bytes.fromString('Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi'),
      Bytes.fromString(loremipsum2),
      592);
  });
});
