// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import request from 'request';
import parse from 'csv-parse';
import {DatasetSpec, List, Struct, newStruct, escapeStructField} from '@attic/noms';

const args = argv
  .usage('Usage: $0 <url> <dataset>')
  .command('url', 'url to import')
  .command('dataset', 'dataset spec to write to')
  .demand(2)
  .argv;

const clearLine = '\x1b[2K\r';
const startTime = Date.now();

main();

function main() {
  const url = args._[0];
  const spec = DatasetSpec.parse(args._[1]);
  if (!spec) {
    process.stderr.write('invalid dataset spec');
    process.exit(1);
    return;
  }

  const ds = spec.dataset();

  let listP = Promise.resolve(new List([]));
  const parser = parse({columns: true});
  let i = 0;

  parser.on('readable', () => {
    let record;
    while ((record = parser.read())) {
      i++;
      const struct = newStruct('', specializeRecordTypes(record));
      listP = listP.then(list => {
        process.stdout.write(`${clearLine}${i} rows`);
        return list.append(struct);
      });
    }
  });

  parser.on('error', err => {
    process.stderr.write(`${err.message}\n`);
    process.exit(1);
  });

  parser.on('finish', () => {
    listP.then(list => {
      process.stdout.write(`${clearLine}Committing ${list.length} rows\n`);
      return ds.commit(list);
    }).then(() => {
      const elapsed = (Date.now() - startTime) / 1000;
      process.stdout.write(`${clearLine}Wrote ${i} rows in ${elapsed}s\n`);
      process.exit(0);
    }).catch(err => {
      process.stderr.write(`${err.message}\n`);
      process.exit(1);
    });
  });

  request(url)
    .on('response', res => {
      if (res.statusCode >= 400) {
        process.stderr.write(`Error fetching ${url}: ${res.statusCode}: ${res.statusMessage}\n`);
        process.exit(1);
      }
    })
    .pipe(parser);
}

function specializeType(s: string): number | string {
  // Prevent empty strings from being treated as 0.
  if (/^\s*$/.test(s)) {
    return s;
  }
  const n = Number(s);
  if (!isNaN(n)) {
    return n;
  }
  return s;
}

function cleanupName(s: string): string {
  // Disallow field names that conflict with the Value/Struct API.
  if (s in Struct.prototype) {
    s += '_';
  }
  return escapeStructField(s);
}

function specializeRecordTypes(obj) {
  const newObject = Object.create(null);
  Object.keys(obj).forEach(key => {
    newObject[cleanupName(key)] = specializeType(obj[key]);
  });
  return newObject;
}
