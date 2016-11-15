// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  PathSpec,
  getTypeOfValue,
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
    'Usage: node . <in-object> <out-dataset>')
  .demand(2)
  .option('source-tags', {
    describe: 'comma-separated list of source tags to write into created photos',
    type: 'string',
  })
  .argv;

const sizes = ['t', 's', 'm', 'l', 'o'];
const flickrNum = makeUnionType([stringType, numberType]);
const sizeTypes = sizes.map(s =>
  makeStructType('', {
    ['height_' + s]: flickrNum,
    ['width_' + s]: flickrNum,
    ['url_' + s]: stringType,
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
const imageType = makeUnionType(sizeTypes.map(st => {
  const newFields = {
    id: stringType,
    title: stringType,
    tags: stringType,
    latitude: flickrNum,
    longitude: flickrNum,
    datetaken: stringType,
    datetakenunknown: flickrNum,
    dateupload: flickrNum,
    lastupdate: flickrNum,
  };
  st.desc.forEachField((name, type) => {
    newFields[name] = type;
  });

  return makeStructType('', newFields);
}));

const nsInSecond = 1e9;
const nsInMillisecond = 1e6;
const clearLine = '\x1b[2K\r';

main().catch(ex => {
  console.error(ex);
  process.exit(1);
});

async function main(): Promise<void> {
  const inSpec = PathSpec.parse(args._[0]);
  const pinnedSpec = await inSpec.pin();
  if (!pinnedSpec) {
    throw `Input dataset ${inSpec.path.dataset} does not exist`;
  }
  const [db, input] = await pinnedSpec.value();
  if (!input) {
    throw `Input spec ${args._[0]} does not exist`;
  }

  const sourceTags = new Set(args['source-tags'] ? args['source-tags'].split(',') : []);
  const outSpec = DatasetSpec.parse(args._[1]);
  const [outDB, output] = outSpec.dataset();
  let result = Promise.resolve(new Set());

  // TODO: How to report progress?
  await walk(input, db, (v: any) => {
    if (isSubtype(imageType, getTypeOfValue(v))) {
      const photo: Object = {
        id: `https://github.com/attic-labs/noms/samples/js/flickr/find-photos#${v.id}`,
        datePublished: newDate(Number(v.dateupload) * nsInSecond),
        dateUpdated: newDate(Number(v.lastupdate) * nsInSecond),
        resources: getResources(v),
        sizes: getSizes(v),
        sources: sourceTags,
        tags: new Set(v.tags ? v.tags.split(' ') : []),
        title: v.title,
      };

      if (!v.datetakenunknown) {
        photo.dateTaken = newDate(Date.parse(v.datetaken) * nsInMillisecond);
      }

      // Flickr API always includes a geoposition, but sometimes it is zero.
      const geo = (getGeo(v): Object);
      if (geo.latitude !== 0 && geo.longitude !== 0) {
        photo.geoposition = geo;
      }

      result = result
          .then(r => r.add(newStruct('Photo', photo)))
          .then(r => {
            process.stdout.write(clearLine + `Indexed ${r.size} photos...`);
            return r;
          });
      return true;
    }
    return false;
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

function getGeo(input: Object): Struct {
  return newStruct('Geoposition', {
    latitude: Number(input.latitude),
    longitude: Number(input.longitude),
  });
}

function getSizes(input: Object): Map<Struct, string> {
  const a = sizes.map((s, i) => {
    if (!isSubtype(sizeTypes[i], input.type)) {
      return null;
    }
    const url = input['url_' + s];
    const width = Number(input['width_' + s]);
    const height = Number(input['height_' + s]);
    return [newStruct('', {width, height}), url];
  });
  // $FlowIssue: Does not understand that filter removes all null values.
  return new Map(a.filter(kv => kv));
}

function getResources(input: Object): Map<Struct, Struct> {
  const a = sizes.map((s, i) => {
    if (!isSubtype(sizeTypes[i], input.type)) {
      return null;
    }
    const url = input['url_' + s];
    const width = Number(input['width_' + s]);
    const height = Number(input['height_' + s]);
    return [newStruct('', {width, height}), url];
  });
  // $FlowIssue: Does not understand that filter removes all null values.
  return new Map(a.filter(kv => kv));
}

function newDate(nsSinceEpoch: number): Struct {
  return newStruct('Date', {nsSinceEpoch});
}
