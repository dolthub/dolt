// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  Database,
  Dataset,
  DatasetSpec,
} from '@attic/noms';

const args = argv
  .usage('Usage: $0 <dataset>')
  .command('dataset', 'dataset to read/write')
  .demand(1)
  .argv;

main().catch(ex => {
  console.error(ex.stack);
  process.exit(1);
});

async function main(): Promise<void> {
  const spec = DatasetSpec.parse(args._[0]);
  if (!spec) {
    process.stderr.write('invalid dataset spec');
    process.exit(1);
    return;
  }

  const [db, ds] = spec.dataset();
  await increment(db, ds);
}

async function increment(db: Database, ds: Dataset): Promise<Dataset> {
  let lastVal = 0;

  const value = await ds.headValue();
  if (value !== null) {
    lastVal = Number(value);
  }

  const newVal = lastVal + 1;
  ds = await db.commit(ds, newVal);
  process.stdout.write(`${ newVal }\n`);
  return ds;
}
