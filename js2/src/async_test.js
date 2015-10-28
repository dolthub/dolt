/* @flow */

'use strict';

const {test: mtest} = require('mocha');

function test(n: string, f: () => ?Promise) {
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

module.exports = test;
