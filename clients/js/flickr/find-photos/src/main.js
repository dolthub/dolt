// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  isSubtype,
  makeStructType,
  makeUnionType,
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
    'Indexes Photo objects out of slurped Flickr metadata\n\n' +
    'Usage: flickr-find-photos <in-object> <out-dataset>')
  .demand(2)
  .argv;

const sizes = ['t', 's', 'm', 'l', 'o'];
const flickrNum = makeUnionType([stringType, numberType]);
const sizeTypes = sizes.map(s =>
  makeStructType('', {
    ['url_' + s]: stringType,
    ['width_' + s]: flickrNum,
    ['height_' + s]: flickrNum,
  }));

// This is effectively:
// union {
//   struct {
//     title: string,
//     tags: string,
//     latitude: flickrNum,
//     longitude: flickrNum,
//     url_t: string,
//     width_t: flickrNum,
//     height_t: flickrNum,
//   } |
//   ... for all the image size suffixes ...
// }
const imageType = makeUnionType(sizeTypes.map(st =>
    makeStructType('', Object.assign(({
      title: stringType,
      tags: stringType,
      latitude: flickrNum,
      longitude: flickrNum,
    }:Object), st.desc.fields))));

main().catch(ex => {
  console.error(ex);
  process.exit(1);
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

  // TODO: How to report progress?
  await walk(input, output.database, v => {
    if (v instanceof Struct && isSubtype(imageType, v.type)) {
      const s = newStruct('Photo', {
        title: v.title || '',
        tags: new Set(v.tags ? v.tags.split(' ') : []),
        geoposition: getGeo(v),
        sizes: getSizes(v),
      });
      result = result.then(r => r.insert(s));
      return true;
    }
    return false;
  });

  return output.commit(await result).then();
}

function getGeo(input: Object): Struct {
  const geopos = {
    latitude: Number(input.latitude || 0),
    longitude: Number(input.longitude || 0),
  };
  return newStruct('Geoposition', geopos);
}

function getSizes(input: Object): Map<Struct, string> {
  return new Map(
    sizes.map((s, i) => {
      if (!isSubtype(sizeTypes[i], input.type)) {
        // $FlowIssue - Flow doesn't realize that filter will return only non-nulls.
        return null;
      }
      const url = input['url_' + s];
      const width = Number(input['width_' + s]);
      const height = Number(input['height_' + s]);
      return [newStruct('', {width, height}), url];
    }).filter(kv => kv));
}
