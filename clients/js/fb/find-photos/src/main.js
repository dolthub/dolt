// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  invariant,
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
    'Usage: fb/find-photos <in-object> <out-dataset>')
  .demand(2)
  .argv;

main().catch(ex => {
  console.error(ex);
  process.exit(1);
});

const imageType = makeStructType('', {
  source: stringType,
  width: numberType,
  height: numberType,
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
  const outSpec = DatasetSpec.parse(args._[1]);
  if (!inSpec) {
    throw 'invalid input object spec';
  }
  if (!outSpec) {
    throw 'inalid output dataset spec';
  }

  const input = await inSpec.value();
  const output = outSpec.dataset();
  let result = Promise.resolve(new Set());

  // TODO: progress
  await walk(input.photos, inSpec.database.database(), async (v: any) => {
    if (v instanceof Struct && isSubtype(photoType, v.type)) {
      const photo: Object = {
        title: v.name || '',
        sizes: await getSizes(v),
        tags: new Set(),  // fb has 'tags', but they are actually people not textual tags
      };
      if (isSubtype(placeType, v.type)) {
        photo.geoposition = getGeo(v);
      }
      result = result.then(r => r.insert(newStruct('Photo', photo)));
      return true;
    }
  });

  return output.commit(await result).then();
}

function getGeo(input: Struct): Struct {
  invariant(input.place);
  return newStruct('Geoposition', {
    latitude: input.place.location.latitude,
    longitude: input.place.location.longitude,
  });
}

async function getSizes(input: Struct): Promise<Map<Struct, string>> {
  let result = Promise.resolve(new Map());
  invariant(input.images);
  await input.images.forEach(v => {
    result = result.then(m => m.set(
      newStruct('', {width: v.width, height: v.height}),
      v.source));
  });
  return result;
}
