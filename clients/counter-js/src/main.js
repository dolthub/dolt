// @flow

import argv from 'yargs';
import {
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

  const ds = spec.set();
  await increment(ds);
}

async function increment(ds: Dataset): Promise<Dataset> {
  let lastVal = 0;

  const commit = await ds.head();
  if (commit !== null && commit !== undefined) {
    lastVal = Number(commit.value);
  }

  const newVal = lastVal + 1;

  process.stdout.write(`\nincrementing counter to ${ newVal }\n`);
  return ds.commit(newVal);
}
