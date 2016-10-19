// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import http from 'http';
import humanize from 'humanize';
import {
  BlobWriter,
  DatasetSpec,
  invariant,
  Blob,
} from '@attic/noms';

const args = argv
  .usage('Usage: $0 <url> <dataset>')
  .command('url', 'url to import')
  .command('dataset', 'dataset spec to write to')
  .demand(2)
  .argv;

const clearLine = '\x1b[2K\r';
const startTime = Date.now();

let expectedBytes = 0;
let expectedBytesHuman = '';
let completedBytes = 0;

main().catch(ex => {
  console.error(ex.stack);
  process.exit(1);
});

function main(): Promise<void> {
  const url = args._[0];
  const spec = DatasetSpec.parse(args._[1]);
  if (!spec) {
    process.stderr.write('invalid dataset spec');
    process.exit(1);
    return Promise.resolve();
  }

  const [db, set] = spec.dataset();
  return getBlob(url)
    .then(b => db.commit(set, b))
    .then(() => {
      process.stderr.write(clearLine + 'Done\n');
    });
}

function getBlob(url): Promise<Blob> {
  const w = new BlobWriter();

  return new Promise(resolve => {
    http.get(url, res => {
      switch (Math.floor(res.statusCode / 100)) {
        case 4:
        case 5:
          invariant(typeof res.statusMessage === 'string');
          process.stderr.write(`Error fetching ${url}: ${res.statusCode}: ${res.statusMessage}\n`);
          process.exit(1);
          break;
      }

      process.stdout.write(clearLine + `got ${res.statusCode}, continuing...\n`);

      const header = res.headers['content-length'];
      if (header) {
        expectedBytes = Number(header);
        expectedBytesHuman = humanize.filesize(expectedBytes);
      } else {
        expectedBytesHuman = '(unknown)';
      }

      res.on('error', e => {
        process.stderr.write(`Error fetching ${url}: ${e.message}`);
        process.exit(1);
      });

      res.on('data', chunk => {
        w.write(chunk);
        completedBytes += chunk.length;
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = humanize.filesize(completedBytes / elapsed);
        process.stdout.write(clearLine + `${humanize.filesize(completedBytes)} of ` +
            `${expectedBytesHuman} written in ${elapsed}s (${rate}/s)`);
      });

      res.on('end', () => {
        process.stdout.write(clearLine + 'Committing...');
        w.close();
        resolve(w.blob);
      });

      res.resume();
    });
  });
}
