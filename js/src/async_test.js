/* @flow */

'use strict';

import {test as mtest} from 'mocha';

export default function test(n: string, f: () => ?Promise) {
  mtest(n, done => {
    try {
      const p = f();
      if (p instanceof Promise) {
        p.then(done, done);
      } else {
        done();
      }
    } catch (ex) {
      done(ex);
    }
  });
}
