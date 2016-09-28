// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import {
  DatasetSpec,
  isSubtype,
  makeListType,
  makeStructType,
  Map,
  newStruct,
  numberType,
  Set,
  stringType,
  Struct,
  walk,
} from '@attic/noms';

const args = argv
  .usage(
    'Finds photos in slurped Facebook metadata\n\n' +
    'Usage: node . <in-dataset> <out-dataset>')
  .demand(2)
  .argv;

main().catch(ex => {
  console.error(ex);
  process.exit(1);
});

const imageType = makeStructType('', {
  height: numberType,
  width: numberType,
  source: stringType,
});

const photoType = makeStructType('', {
  id: stringType,
  images: makeListType(imageType),
});

const placeType = makeStructType('', {
  place: makeStructType('', {
    location: makeStructType('', {
      latitude: numberType,
      longitude: numberType,
    }),
  }),
});

async function main(): Promise<void> {
  const inSpec = DatasetSpec.parse(args._[0]);
  const [db, input] = await inSpec.value();
  if (!input) {
    return db.close();
  }
  const outSpec = DatasetSpec.parse(args._[1]);
  const [outDB, output] = outSpec.dataset();
  let result = Promise.resolve(new Set());

  // TODO: progress
  await walk(input, db, async (v: any) => {
    if (v instanceof Struct && isSubtype(photoType, v.type)) {
      const photo: Object = {
        title: v.name || '',
        sizes: await getSizes(v),
        tags: new Set(),  // fb has 'tags', but they are actually people not textual tags
      };
      if (isSubtype(placeType, v.type)) {
        photo.geoposition = getGeo(v);
      }
      result = result.then(r => r.add(newStruct('Photo', photo)));
      return true;
    }
  });

  return outDB.commit(output, await result)
    .then(() => db.close())
    .then(() => outDB.close());
}

function getGeo(input): Struct {
  return newStruct('Geoposition', {
    latitude: input.place.location.latitude,
    longitude: input.place.location.longitude,
  });
}

async function getSizes(input): Promise<Map<Struct, string>> {
  let result = Promise.resolve(new Map());
  await input.images.forEach(v => {
    result = result.then(m => m.set(
      newStruct('', {width: v.width, height: v.height}),
      v.source));
  });
  return result;
}
