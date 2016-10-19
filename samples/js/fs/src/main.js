// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import fs from 'mz/fs';
import humanize from 'humanize';
import path from 'path';
import argv from 'yargs';
import {
  Blob,
  blobType,
  BlobWriter,
  createStructClass,
  DatasetSpec,
  Database,
  makeCycleType,
  makeMapType,
  makeRefType,
  makeStructType,
  makeUnionType,
  Map,
  Ref,
  stringType,
} from '@attic/noms';

const clearLine = '\x1b[2K\r';

const args = argv
  .usage('Usage: $0 <path> <dataset>')
  .command('path', 'filesystem path to import')
  .demand(1)
  .command('dataset', 'dataset to write to')
  .demand(1)
  .argv;

// struct File {
//   content: Ref<Blob>,
// }
//
// struct Directory {
//   entries: Map<String, Cycle<0> | File>,
// }

const fileType = makeStructType('File', {
  content: makeRefType(blobType),
});
const directoryType = makeStructType('Directory', {
  entries: makeMapType(stringType, makeUnionType([fileType, makeCycleType(0)])),
});

const File = createStructClass(fileType);
const Directory = createStructClass(directoryType);

let numFilesFound = 0;
let numFilesComplete = 0;
let sizeFilesFound = 0;
let sizeFilesComplete = 0;
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
  const spec = DatasetSpec.parse(args._[1]);
  if (!spec) {
    process.stderr.write('invalid dataset spec');
    process.exit(1);
    return;
  }

  startTime = Date.now();

  const [db, ds] = spec.dataset();
  const de = await processPath(path, db);
  if (de) {
    await db.commit(ds, de);
    process.stdout.write('\ndone\n');
  }
}

async function processPath(p: string, store: Database): Promise<void | Directory | File> {
  numFilesFound++;
  const st = await fs.stat(p);
  sizeFilesFound += st.size;
  let de = null;
  if (st.isDirectory()) {
    de = await processDirectory(p, store);
  } else if (st.isFile()) {
    de = await processFile(p, store);
  } else {
    console.info('Skipping path %s because this filesystem node type is not currently handled', p);
    return null;
  }
  return de;
}

async function processDirectory(p: string, store: Database): Promise<Directory> {
  const names = await fs.readdir(p);
  const children = names.map(name => {
    const chPath = path.join(p, name);
    return processPath(chPath, store).then(dirEntry => {
      if (!dirEntry) {
        return null;
      }
      return [name, dirEntry];
    });
  }).filter(x => x);

  numFilesComplete++;
  updateProgress();

  const entries = new Map(
    (await Promise.all(children))
      .filter(([, dirEntry]) => dirEntry));

  return new Directory({entries});
}

async function processFile(p: string, store: Database): Promise<File> {
  const f = new File({
    content: await processBlob(p, store),
  });
  numFilesComplete++;
  updateProgress();
  return f;
}

function processBlob(p: string, store: Database): Promise<Ref<Blob>> {
  const w = new BlobWriter(store);
  const s = fs.createReadStream(p);
  return new Promise((res, rej) => {
    s.on('data', chunk => {
      sizeFilesComplete += chunk.length;
      w.write(chunk);
      updateProgress();
    });
    s.on('end', async () => {
      try {
        w.close();
        const blob = await w.blob;
        res(store.writeValue(blob));
      } catch (ex) {
        rej(ex);
      }
    });
    s.on('error', rej);
  });
}

function updateProgress() {
  const elapsed = Date.now() - startTime;
  const rate = sizeFilesComplete / (elapsed / 1000);
  process.stdout.write(`${clearLine}${numFilesComplete} of ${numFilesFound} entries ` +
    `(${humanize.filesize(sizeFilesComplete)} of ${humanize.filesize(sizeFilesFound)} - ` +
    `${humanize.filesize(rate)}/s) processed...`);
}
