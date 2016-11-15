// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  createStructClass,
  DatasetSpec,
  PathSpec,
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
    'Usage: node . <in-path> <out-dataset>')
  .demand(2)
  .option('source-tags', {
    describe: 'comma-separated list of source tags to write into created photos',
    type: 'string',
  })
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

const clearLine = '\x1b[2K\r';

async function main(): Promise<void> {
  const inSpec = PathSpec.parse(args._[0]);
  const pinnedSpec = await inSpec.pin();
  if (!pinnedSpec) {
    throw `Invalid input dataset: ${inSpec.path.dataset}`;
  }
  const [db, input] = await pinnedSpec.value();
  if (!input) {
    throw `Invalid input spec: ${inSpec.toString()}`;
  }
  const sourceTags = new Set(args['source-tags'] ? args['source-tags'].split(',') : []);
  const outSpec = DatasetSpec.parse(args._[1]);
  const [outDB, output] = outSpec.dataset();
  let result = Promise.resolve(new Set());

  // TODO: progress
  await walk(input, db, async (v: any) => {
    if (v instanceof Struct && isSubtype(photoType, v.type)) {
      const photo: Object = {
        id: `https://github.com/attic-labs/noms/samples/js/fb/find-photos#${v.id}`,
        datePublished: new NomsDate({nsSinceEpoch: v.created_time * 1e9}),
        dateUpdated: new NomsDate({nsSinceEpoch: v.updated_time * 1e9}),
        resources: await getResources(v),
        sizes: await getSizes(v),
        sources: sourceTags,
        tags: new Set(),  // fb has 'tags', but they are actually people not textual tags
        title: v.name || '',
      };
      if (isSubtype(placeType, v.type)) {
        photo.geoposition = getGeo(v);
      }
      if (isSubtype(tagsType, v.type)) {
        photo.facesCentered = await getFaces(v);
      }
      result = result
          .then(r => r.add(newStruct('Photo', photo)))
          .then(r => {
            process.stdout.write(clearLine + `Indexed ${r.size} photos...`);
            return r;
          });
      return true;
    }
  });

  return outDB.commit(output, await result, {
    meta: newStruct('', {
      date: new Date().toISOString(),
      input: pinnedSpec.toString(),
    }),
  })
  .then(() => db.close())
  .then(() => outDB.close())
  .then(() => {
    process.stdout.write(clearLine);
  });
}

function getGeo(input): Struct {
  return newStruct('Geoposition', {
    latitude: input.place.location.latitude,
    longitude: input.place.location.longitude,
  });
}

async function getSizes(input): Promise<Map<Struct, string>> {
  const tuples = [];
  await input.images.forEach(v => {
    tuples.push([
      newStruct('', {width: v.width, height: v.height}),
      v.source,
    ]);
  });
  return new Map(tuples);
}

async function getResources(input): Promise<Map<Struct, Struct>> {
  const tuples = [];
  await input.images.forEach(v => {
    tuples.push([
      newStruct('', {width: v.width, height: v.height}),
      newStruct('RemoteResource', {url: v.source}),
    ]);
  });
  return new Map(tuples);
}

async function getFaces(photo): Promise<Set<Struct>> {
  // TODO: Facebook only gives us the centerpoint of each face, not the
  // bounding box. Ideally, we'd use face detection on the image to get the
  // real bounding box, but for now, we just guess that faces are typically
  // about 1/3 the width/height of image.
  //
  // This fails badly in lots of cases though, so we should fix asap.
  const result = [];
  await photo.tags.data.forEach(v => {
    const x = v.x / 100;
    const y = v.y / 100;
    result.push(newStruct('', {x, y, name: v.name}));
  });
  return new Set(result);
}
