// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import humanize from 'humanize';
import argv from 'yargs';

import {BinaryEncoderDecoder} from './binary-encoder.js';
import {BinaryIntEncoderDecoder} from './binary-int-encoder.js';
import {BinaryVarintEncoderDecoder} from './binary-varint-encoder.js';
import {StringEncoderDecoder} from './string-encoder.js';

const args = argv
  .usage('Usage: $0 [options]')
  .option('from', {
    describe: 'start iterations from this number',
    type: 'number',
    default: 1e2,
  })
  .option('to', {
    describe: 'run iterations until arriving at this number',
    type: 'number',
    default: 1e4,
  })
  .option('by', {
    describe: 'increment each iteration by this number',
    type: 'number',
    default: 1,
  })
  .option('encoding', {
    description: 'encode/decode as \'string\',\'binary\', \'binary-int\`, or \`binary-varint\'',
    type: 'string',
    default: 'string',
  })
  .argv;

let numBytes = 0;
let numIterations = 0;
let startTime = 0;

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

function getEncoder(name) {
  if (name === 'string') {
    return new StringEncoderDecoder();
  } else if (name === 'binary') {
    return new BinaryEncoderDecoder();
  } else if (name === 'binary-int') {
    return new BinaryIntEncoderDecoder();
  } else if (name === 'binary-varint') {
    return new BinaryVarintEncoderDecoder();
  } else {
    console.error(`unknown encoding option: ${args.encoding}`);
    process.exit(1);
  }
  throw new Error('unreachable');
}

async function main(): Promise<void> {
  startTime = Date.now();
  if (console.profile) {
    console.profile('encode');
  }

  const encoder = getEncoder(args.encoding);

  console.log(`enc: ${args.encoding} from: ${args.from} to: ${args.to} by: ${args.by}`);
  for (let i = args.from; i < args.to; i += args.by) {
    numIterations++;
    const buf = new Buffer(256);
    const len = encoder.encode(buf, i);
    numBytes += len * 2;
    const decodeBuf = buf.slice(0, len);
    const j = encoder.decode(decodeBuf);
    if (i !== j) {
      console.log(`${i} != ${j}`);
    }
  }
  if (console.profileEnd) {
    console.profileEnd('encode');
  }
  const elapsed = Date.now() - startTime;
  const rate = numBytes / (elapsed / 1000);
  console.log(`IO ${humanize.filesize(numBytes)} ` +
    `(${numIterations} nums) ` +
    `(${humanize.filesize(rate)}/s) processed...`);
}
