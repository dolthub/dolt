// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  invariant,
  makeRefType,
  makeSetType,
  newSet,
  Struct,
  walk,
} from '@attic/noms';

const args = argv
  .usage('Usage: $0 -n <name> <input-dataset> <output-dataset>')
  .demand(2)
  .option('n', {
    alias: 'name',
    describe: 'struct name to search for',
    type: 'string',
    demand: true,
  })
  .argv;

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

async function main(): Promise<void> {
  const inSpec = DatasetSpec.parse(args._[0]);
  invariant(inSpec, quit('invalid input dataset spec'));
  const outSpec = DatasetSpec.parse(args._[1]);
  invariant(outSpec, quit('invalid input dataset spec'));

  const input = inSpec.set();
  const rv = await input.headRef();
  if (!rv) {
    process.stderr.write(`{args._[0]} does not exist}\n`);
    return;
  }

  const commit = await rv.targetValue(input.store);
  const output = outSpec.set();

  let s, elemType;
  
  await walk(commit.value, input.store, async cv => {
    if (!(cv instanceof Struct) || cv.type.name !== argv.name) {
      return false;
    }

    // TODO(aa): Remove this business once structural typing is in place.
    if (!s) {
      elemType = cv.type;
      s = newSet([], makeSetType(makeRefType(elemType)));
    } else if (!cv.type.equals(elemType)) {
      throw new Error('Not implemented: cannot supported mixed collections (yet)');
    }

    const rv = output.store.writeValue(cv);

    // This is tricksy. We can't use await because we need the set insertions to happen in serial,
    // because otherwise (due to immutable datastructures), we will lose some of the inserts.
    s = s.then(s => s.insert(rv));

    return s.then(() => false);
  });

  // TODO(aa): Remove this when we have structural typing (commit empty set instead).
  if (s) {
    await output.commit(await s);
  }
}

function quit(err: string): Function {
  return () => {
    process.stderr.write(err);
    process.exit(1);
  };
}
