// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  invariant,
  Map,
  Struct,
  StructMirror,
  walk,
} from '@attic/noms';

const args = argv
  .usage('Usage: aggregate -struct <struct-name> -group-by <field-name> ' +
         '[-function sum] <input-dataset> <output-dataset>')
  .demand(2)
  .option('struct', {
    alias: 's',
    describe: 'struct name to search for',
    type: 'string',
    demand: true,
  })
  .option('groupby', {
    alias: 'g',
    describe: 'field name to group on',
    type: 'string',
    demand: true,
  })
  .option('function', {
    alias: 'f',
    describe: 'function to aggregate by',
    type: 'string',
    default: 'sum',
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

  const [, input] = inSpec.dataset();
  const rv = await input.headRef();
  if (!rv) {
    process.stderr.write(`{args._[0]} does not exist}\n`);
    return;
  }

  const commit = await rv.targetValue(input.database);

  let out = Promise.resolve(new Map());

  await walk(commit.value, input.database, cv => {
    if (!(cv instanceof Struct) || cv.type.desc.name !== args.struct) {
      return;
    }

    const fv = new StructMirror(cv).get(args.groupby);
    if (!fv) {
      return;
    }

    // This is tricksy. We can't use await because we need the set insertions to happen in serial,
    // because otherwise (due to immutable datastructures), we will lose some of the inserts.
    out = out
      .then(m => m.get(fv)
        .then(prev => m.set(fv, (prev || 0) + 1)));

    return true;
  });

  const [db, output] = outSpec.dataset();
  await db.commit(output, await out);
}

function quit(err: string): Function {
  return () => {
    process.stderr.write(err + '\n');
    process.exit(1);
  };
}
