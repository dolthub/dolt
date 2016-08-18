// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import crypto from 'crypto';
import fs from 'mz/fs';
import humanize from 'humanize';
import argv from 'yargs';

import {BuzHash} from '@attic/noms';

const clearLine = '\x1b[2K\r';

const args = argv
  .usage('Usage: $0 <big-file>')
  .command('big-file', 'filesystem path to import')
  .demand(1)
  .option('use-sha1', {
    alias: 'sha1',
    describe: 'whether to sha1 or not',
    type: 'boolean',
    default: false,
  })
  .option('read-bytes', {
    alias: 'rb',
    description: 'read bytes off the stream, but don\'t do anything with them',
    type: 'boolean',
    deafult: false,
  })
  .option('use-buzhash', {
    alias: 'bh',
    describe: 'whether to buzhash or not - implies -read-bytes',
    type: 'boolean',
    default: false,
  })
  .argv;

let fileSize = 0;
let startTime = 0;

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

async function main(): Promise<void> {
  const path = args._[0];
  startTime = Date.now();
  const digest = await processFile(path);
  console.log('\ndone', digest);
}

function processFile(p: string): Promise<?string> {
  const h = crypto.createHash('sha1');
  const bh = new BuzHash(64 * 8);

  if (console.profile) {
    console.profile('hash');
  }

  const s = fs.createReadStream(p);
  return new Promise((res, rej) => {
    s.on('data', chunk => {
      if (args['use-sha1']) {
        h.update(chunk);
      }
      if (args['read-bytes'] || args['use-buzhash']) {
        for (let i = 0; i < chunk.length; i ++) {
          if (args['use-buzhash']) {
            bh.hashByte(chunk[i]);
          }
        }
      }
      fileSize += chunk.length;
    });
    s.on('end', async () => {
      res(args['use-sha1'] ? h.digest('hex') : '<use-sha1 not-enabled>');
      updateProgress();
      if (console.profileEnd) {
        console.profileEnd('hash');
      }
    });
    s.on('error', rej);
  });
}

function updateProgress() {
  const elapsed = Date.now() - startTime;
  const rate = fileSize / (elapsed / 1000);
  process.stdout.write(`${clearLine}` +
    `(${humanize.filesize(fileSize)} of ${humanize.filesize(fileSize)} - ` +
    `${humanize.filesize(rate)}/s) processed...`);
}
