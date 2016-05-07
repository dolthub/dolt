// @flow

import fs from 'mz/fs';
import humanize from 'humanize';
import path from 'path';
import argv from 'yargs';
import {
  blobType,
  BlobWriter,
  createStructClass,
  DatasetSpec,
  Database,
  makeMapType,
  makeRefType,
  makeStructType,
  newMap,
  NomsBlob,
  RefValue,
  stringType,
  valueType,
  Value,
} from '@attic/noms';

const clearLine = '\x1b[2K\r';

const args = argv
  .usage('Usage: $0 <path> <dataset>')
  .command('path', 'filesystem path to import')
  .demand(1)
  .command('dataset', 'dataset to write to')
  .demand(1)
  .argv;

// TODO(aa): Change to Map<string, File|Directory> once we have unions.
const entriesType = makeMapType(stringType, valueType);
const File = createStructClass(makeStructType('File', {
  content: makeRefType(blobType),
}));
const Directory = createStructClass(makeStructType('Directory', {
  entries: makeRefType(entriesType),
}));

async function newDirectoryEntryMap(values) {
  return newMap(values, entriesType);
}

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

  const ds = spec.set();
  const de = await processPath(path, ds.store);
  if (de) {
    await ds.commit(de);
    process.stdout.write('\ndone\n');
  }

}

async function processPath(p: string, store: Database): Promise<?Value> {
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
    return processPath(chPath, store).then(dirEntry => [name, dirEntry]);
  });

  numFilesComplete++;
  updateProgress();

  const resolved = await Promise.all(children);
  const entries = resolved
    .filter(([, dirEntry]) => dirEntry)
    .reduce((l, t) => { l.push(...t); return l; }, []);
  const fm = await newDirectoryEntryMap(entries);
  return new Directory({
    entries: store.writeValue(fm),
  });
}

async function processFile(p: string, store: Database): Promise<File> {
  const f = new File({
    content: await processBlob(p, store),
  });
  numFilesComplete++;
  updateProgress();
  return f;
}


function processBlob(p: string, store: Database): Promise<RefValue<NomsBlob>> {
  const w = new BlobWriter();
  const s = fs.createReadStream(p);
  return new Promise((res, rej) => {
    s.on('data', chunk => {
      sizeFilesComplete += chunk.length;
      w.write(chunk);
      updateProgress();
    });
    s.on('end', async () => {
      await w.close();
      try {
        res(store.writeValue(w.blob));
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
