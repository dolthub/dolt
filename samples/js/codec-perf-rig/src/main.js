// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import humanize from 'humanize';

import {
  Blob,
  DatasetSpec,
  List,
  Map,
  notNull,
  Set,
  boolType,
  makeStructType,
  newStructWithType,
  numberType,
  stringType,
} from '@attic/noms';
import type {Collection, Value} from '@attic/noms';

const numberSize = 8;
const strPrefix = 'i am a 32 bytes.....'; // TODO
const stringSize = 32;
const structSize = 64;

const args = argv
  .usage('Usage: $0')
  .option('count', {
    describe: 'number of elements',
    type: 'number',
    default: 10000,
  })
  .option('blobsize', {
    describe: 'size of blob of create',
    type: 'number',
    default: 2 << 20 /* 2 MB */,
  })
  .demand(0)
  .argv;

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

async function main(): Promise<void> {
  const buildCount = args['count'];
  const blobSize = args['blobsize'];
  const insertCount = buildCount / 50;

  const collectionTypes = ['List', 'Set', 'Map'];
  const buildFns = [buildList, buildSet, buildMap];
  const buildIncrFns = [buildListIncrementally, buildSetIncrementally, buildMapIncrementally];
  const readFns = [readList, readSet, readMap];

  const elementTypes = ['numbers (8 B)', 'strings (32 B)', 'structs (64 B)'];
  const elementSizes = [numberSize, stringSize, structSize];
  const valueFns = [createNumber, createString, createStruct];

  for (let i = 0; i < collectionTypes.length; i++) {
    console.log(`Testing ${collectionTypes[i]}: \t\tbuild ${buildCount} \t\t\t\scan ${
      buildCount}\t\t\t\insert ${insertCount}`);

    for (let j = 0; j < elementTypes.length; j++) {
      const elementType = elementTypes[j];
      const valueFn = valueFns[j];

      // Build One-Time
      let ds = notNull(DatasetSpec.parse('mem::csv')).dataset();

      let t1 = Date.now();
      let col = buildFns[i](buildCount, valueFn);
      ds = await ds.commit(col);
      const buildDuration = Date.now() - t1;

      // Read
      t1 = Date.now();
      col = notNull(await ds.head()).value;
      await readFns[i](col);
      const readDuration = Date.now() - t1;

      // Build Incrementally
      ds = notNull(DatasetSpec.parse('mem::csv')).dataset();
      t1 = Date.now();
      col = await buildIncrFns[i](insertCount, valueFn);
      ds = await ds.commit(col);
      const incrDuration = Date.now() - t1;

      const elementSize = elementSizes[j];
      const buildSize = elementSize * buildCount;
      const incrSize = elementSize * insertCount;

      console.log(`${elementType}\t\t${rate(buildDuration, buildSize)}\t\t${
        rate(readDuration, buildSize)}\t\t${rate(incrDuration, incrSize)}`);
    }
    console.log();
  }

  const blobSizeStr = humanize.numberFormat(blobSize / 1000000);
  console.log(`Testing Blob: \t\tbuild ${blobSizeStr} MB\t\t\tscan ${blobSizeStr} MB`);

  let ds = notNull(DatasetSpec.parse('mem::csv')).dataset();
  const blobBytes = makeBlobBytes(blobSize);

  let t1 = Date.now();
  let blob = new Blob(blobBytes);
  ds = await ds.commit(blob);
  const buildDuration = Date.now() - t1;

  t1 = Date.now();
  blob = notNull(await ds.head()).value;
  const reader = blob.getReader();
  for (let next = await reader.read(); !next.done; next = await reader.read());
  const readDuration = Date.now() - t1;

  console.log(`\t\t\t${rate(buildDuration, blobSize)}\t\t${rate(readDuration, blobSize)}\n`);
}

type createValueFn = (i: number) => Value;

function rate(d: number, size: number): string {
  const rateStr = humanize.numberFormat(size / (d * 1000));
  return `${d} ms (${rateStr} MB/s)`;
}


function createString(i: number): Value {
  return strPrefix + i;
}

function createNumber(i: number): Value {
  return i;
}

const structType = makeStructType('S1',
  ['bool', 'num', 'str'],
  [
    boolType,
    numberType,
    stringType,
  ]
);

function createStruct(i: number): Value {
  return newStructWithType(structType,
    [
      (i % 2) === 0,
      i,
      'i am a 55 bytes..................................' + i,
    ]
  );
}

function makeBlobBytes(byteLength: number): Uint8Array {
  const ar = new ArrayBuffer(byteLength);
  const ua = new Uint32Array(ar);
  for (let i = 0; i < ua.length; i++) {
    ua[i] = i;
  }

  return new Uint8Array(ar);
}

function buildList(count: number, createFn: createValueFn): Collection<any> {
  const values = new Array(count);
  for (let i = 0; i < count; i++) {
    values[i] = createFn(i);
  }

  return new List(values);
}

async function buildListIncrementally(count: number, createFn: createValueFn):
    Promise<Collection<any>> {
  let l = new List();
  for (let i = 0; i < count; i++) {
    l = await l.insert(i, createFn(i));
  }

  return l;
}

function readList(l: List<any>): Promise<void> {
  return l.forEach(() => {});
}

function buildSet(count: number, createFn: createValueFn): Collection<any> {
  const values = new Array(count);
  for (let i = 0; i < count; i++) {
    values[i] = createFn(i);
  }

  return new Set(values);
}

async function buildSetIncrementally(count: number, createFn: createValueFn):
    Promise<Collection<any>> {
  let s = new Set();
  for (let i = 0; i < count; i++) {
    s = await s.add(createFn(i));
  }

  return s;
}

function readSet(l: Set<any>): Promise<void> {
  return l.forEach(() => {});
}

function buildMap(count: number, createFn: createValueFn): Collection<any> {
  const values = new Array(count);
  for (let i = 0; i < count * 2; i += 2) {
    values[i] = [createFn(i), createFn(i + 1)];
  }

  return new Map(values);
}

async function buildMapIncrementally(count: number, createFn: createValueFn):
    Promise<Collection<any>> {
  let m = new Map();
  for (let i = 0; i < count * 2; i += 2) {
    m = await m.set(createFn(i), createFn(i + 1));
  }

  return m;
}

function readMap(l: Map<any, any>): Promise<void> {
  return l.forEach(() => {});
}
