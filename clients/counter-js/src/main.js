// @flow

import argv from 'yargs';
import {
  Dataset,
  DataStore,
  HttpStore,
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
  const [datastoreSpec, datasetName] = parseArgs();
  if (!datastoreSpec) {
    process.exit(1);
    return;
  }

  const store = new DataStore(new HttpStore(datastoreSpec));
  let ds = new Dataset(store, datasetName);

  ds = await increment(ds);
}

async function increment(ds: Dataset): Promise<Dataset> {
  let lastVal = 0;

  const commit = await ds.head();
  if (commit !== null && commit !== undefined) {
    lastVal = Number(commit.value);
  }

  const newVal = lastVal + 1;

  process.stdout.write(`\nincrementing counter to ${ newVal }\n`);
  // TODO: fix this to use a number instead of a string, bug
  // https://github.com/attic-labs/noms/issues/1318
  return ds.commit(newVal.toString());
}

function parseArgs() {
  const [datasetSpec] = args._;
  const parts = datasetSpec.split(':');
  if (parts.length < 2) {
    console.error('invalid dataset spec');
    return [];
  }
  const datasetName = parts.pop();
  const datastoreSpec = parts.join(':');
  if (!/^http/.test(datastoreSpec)) {
    console.error('Unsupported datastore type: ', datastoreSpec);
    return [];
  }
  return [datastoreSpec, datasetName];
}
