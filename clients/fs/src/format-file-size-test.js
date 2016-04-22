// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import formatFileSize from './format-file-size.js';

suite('spread-sheet-number', () => {

  function doTest(n, s) {
    test(s, () => {
      assert.equal(formatFileSize(n), s);
    });
  }

  doTest(0, '0B');
  doTest(1, '1B');
  doTest(2, '2B');
  doTest(123, '123B');
  doTest(1023, '1023B');
  doTest(1024, '1KB');
  doTest(1025, '1KB');
  doTest(1.5 * 1024, '1.5KB');
  doTest(1.2345 * 1024, '1.2KB');

  const K = 1024;
  const M = 1024 * K;
  const G = 1024 * M;
  const T = 1024 * G;
  const P = 1024 * T;

  doTest(.95 * M, '972.8KB');
  doTest(M, '1MB');
  doTest(1.23 * M, '1.2MB');

  doTest(.95 * G, '972.8MB');
  doTest(G, '1GB');
  doTest(1.23 * G, '1.2GB');

  doTest(.95 * T, '972.8GB');
  doTest(T, '1TB');
  doTest(1.23 * T, '1.2TB');

  doTest(.95 * P, '972.8TB');
  doTest(P, '1PB');
  doTest(1.23 * P, '1.2PB');
});
