// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import {
  createStructClass,
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
  images: makeListType(imageType),
  'created_time': numberType,
  'updated_time': numberType,
});

const tagsType = makeStructType('', {
  tags: makeStructType('', {
    data: makeListType(
      makeStructType('', {
        name: stringType,
        x: numberType,
        y: numberType,
      })
    ),
  }),
});

const placeType = makeStructType('', {
  place: makeStructType('', {
    location: makeStructType('', {
      latitude: numberType,
      longitude: numberType,
    }),
  }),
});

const NomsDate = createStructClass(
  makeStructType('Date', {nsSinceEpoch: numberType}));

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
        datePublished: new NomsDate({nsSinceEpoch: v.created_time * 1e9}),
        dateUpdated: new NomsDate({nsSinceEpoch: v.updated_time * 1e9}),
      };
      if (isSubtype(placeType, v.type)) {
        photo.geoposition = getGeo(v);
      }
      if (isSubtype(tagsType, v.type)) {
        photo.faces = await getFaces(v);
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

async function getFaces(photo): Promise<Set<Struct>> {
  // TODO: Facebook only gives us the centerpoint of each face, not the
  // bounding box. Ideally, we'd use face detection on the image to get the
  // real bounding box, but for now, we just guess that faces are typically
  // about 1/3 the width/height of image.
  //
  // This fails badly in lots of cases though, so we should fix asap.
  const mw = 0.33;
  const mh = 0.33;
  const result = [];
  await photo.tags.data.forEach(v => {
    const x = Math.max(0, v.x / 100 - mw / 2);
    const y = Math.max(0, v.y / 100 - mh / 2);
    const w = Math.min(mw, 1 - x);
    const h = Math.min(mh, 1 - y);
    result.push(newStruct('', {x, y, w, h, name: v.name}));
  });
  return new Set(result);
}
