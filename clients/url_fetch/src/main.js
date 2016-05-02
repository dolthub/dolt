// @flow

import argv from 'yargs';
import http from 'http';
import humanize from 'humanize';
import {
  BlobWriter,
  Dataset,
  Database,
  invariant,
  HttpStore,
  NomsBlob,
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
  const [url, databaseSpec, datasetName] = parseArgs();
  if (!url) {
    process.exit(1);
    return Promise.resolve();
  }

  const db = new Database(new HttpStore(databaseSpec));
  const ds = new Dataset(db, datasetName);

  return getBlob(url)
    .then(b => ds.commit(b))
    .then(() => {
      process.stderr.write(clearLine + 'Done\n');
    });
}

function getBlob(url): Promise<NomsBlob> {
  const w = new BlobWriter();

  return new Promise(resolve => {
    http.get(url, res => {
      switch (Math.floor(res.statusCode / 100)) {
        case 4:
        case 5:
          invariant(res.statusMessage);
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
        w.close()
          .then(() => resolve(w.blob));
      });

      res.resume();
    });
  });
}

function parseArgs(): [string, string, string] {
  const [p, datasetSpec] = args._;
  const parts = datasetSpec.split(':');
  if (parts.length < 2) {
    console.error('invalid dataset spec');
    return [];
  }
  const datasetName = parts.pop();
  const databaseSpec = parts.join(':');
  if (!/^http/.test(databaseSpec)) {
    console.error('Unsupported database type: ', databaseSpec);
    return [];
  }
  return [p, databaseSpec, datasetName];
}
