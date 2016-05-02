// @flow

import fs from 'mz/fs';
import humanize from 'humanize';
import path from 'path';
import argv from 'yargs';
import {
  newMapOfStringToRefOfDirectoryEntry,
  Directory,
  DirectoryEntry,
  File,
} from './fs.noms.js';
import {
  BlobWriter,
  Dataset,
  Database,
  HttpStore,
  NomsBlob,
  RefValue,
} from '@attic/noms';

const args = argv
  .usage('Usage: $0 <path> <dataset>')
  .command('path', 'filesystem path to import')
  .demand(1)
  .command('dataset', 'dataset to write to')
  .demand(1)
  .argv;

let numFilesFound = 0;
let numFilesComplete = 0;
let sizeFilesFound = 0;
let sizeFilesComplete = 0;
let startTime = 0;

main().catch(ex => {
  console.error(ex.stack);
  process.exit(1);
});

async function main(): Promise<void> {
  const [p, databaseSpec, datasetName] = parseArgs();
  if (!p) {
    process.exit(1);
    return;
  }

  const db = new Database(new HttpStore(databaseSpec));
  const ds = new Dataset(db, datasetName);

  startTime = Date.now();
  const r = await processPath(p, db);
  if (r) {
    await ds.commit(r);
    process.stdout.write('\ndone\n');
  }

}

async function processPath(p: string, db: Database): Promise<?RefValue<DirectoryEntry>> {
  numFilesFound++;
  const st = await fs.stat(p);
  sizeFilesFound += st.size;
  let de = null;
  if (st.isDirectory()) {
    de = new DirectoryEntry({
      directory: await processDirectory(p, db),
    });
  } else if (st.isFile()) {
    de = new DirectoryEntry({
      file: await processFile(p, db),
    });
  } else {
    console.info('Skipping path %s because this filesystem node type is not currently handled', p);
    return null;
  }

  return await db.writeValue(de);
}

async function processDirectory(p: string, db: Database): Promise<Directory> {
  const names = await fs.readdir(p);
  const children = names.map(name => {
    const chPath = path.join(p, name);
    return processPath(chPath, db).then(dirEntryRef => [name, dirEntryRef]);
  });

  numFilesComplete++;
  updateProgress();

  const resolved = await Promise.all(children);
  const entries = resolved
    .filter(([, dirEntryRef]) => dirEntryRef)
    .reduce((l, t) => { l.push(...t); return l; }, []);
  const fm = await newMapOfStringToRefOfDirectoryEntry(entries);
  return new Directory({
    entries: fm,
  });
}

async function processFile(p: string, db: Database): Promise<File> {
  const f = new File({
    content: await processBlob(p, db),
  });
  numFilesComplete++;
  updateProgress();
  return f;
}


function processBlob(p: string, db: Database): Promise<RefValue<NomsBlob>> {
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
        res(db.writeValue(w.blob));
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
  process.stdout.write(`\r${numFilesComplete} of ${numFilesFound} entries ` +
    `(${humanize.filesize(sizeFilesComplete)} of ${humanize.filesize(sizeFilesFound)} - ` +
    `${humanize.filesize(rate)}/s) processed...`);
}

function parseArgs() {
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
