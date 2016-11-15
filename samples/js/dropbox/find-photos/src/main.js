// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import fit from 'aspect-fit';
import {
  DatasetSpec,
  PathSpec,
  getTypeOfValue,
  isSubtype,
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
    'Indexes Photo objects out of slurped Dropbox metadata.\n' +
    'See dropbox/slurp for how to get an access token.\n\n' +
    'Usage: node . [flags] <in-object> <out-dataset>')
  .option('access-token', {
    describe: 'Dropbox oauth access token',
    type: 'string',
    demand: true,
  })
  .option('source-tags', {
    describe: 'comma-separated list of source tags to write into created photos',
    type: 'string',
  })
  .demand(2)
  .argv;

const sourceType = makeStructType('', {
  'client_modified': stringType,
  'id': stringType,
  'media_info': makeStructType('', {
    'metadata': makeStructType('', {
      'dimensions': makeStructType('', {
        'height': numberType,
        'width': numberType,
      }),
      'time_taken': stringType,
    }),
  }),
  'name': stringType,
  'path_display': stringType,
  'server_modified': stringType,
});

const hasLocation = makeStructType('', {
  'location': makeStructType('', {
    'latitude': numberType,
    'longitude': numberType,
  }),
});

const contentHost = 'https://content.dropboxapi.com/2/';

// FTR, Dropbox resizes proportionally to fit inside these rectangles,
// even if the source photo is portrait. Whee.
const sizes = [[32, 32], [64, 64], [128, 128], [640, 480], [1024, 768]];

const nanosPerMilli = 1e6;
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
  await walk(input, db, async (v: any) => {
    if (isSubtype(sourceType, getTypeOfValue(v))) {
      const resources = getResources(v);
      const photo: Object = {
        id: `https://github.com/attic-labs/noms/samples/js/dropbox/find-photos#${v.id}`,
        dateTaken: newDate(v.media_info.metadata.time_taken),
        dateUpdated: newDate(v.server_modified),
        resources: resources,
        sizes: await getSizes(resources),
        sources: sourceTags,
        tags: new Set(),
        title: v.name,
      };

      if (isSubtype(hasLocation, getTypeOfValue(v.media_info.metadata))) {
        photo.geolocation = v.media_info.metadata.location;
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

async function getSizes(resources: Map<Struct, Struct>): Promise<Map<Struct, string>> {
  const tuples = [];
  await resources.forEach((v: any, k: Struct) => {
    tuples.push([k, v.url]);
  });
  return new Map(tuples);
}

function getResources(input: Object): Map<Struct, Struct> {
  const orig = input.media_info.metadata.dimensions;

  const kv = sizes.map(([width, height]) => {
    const resized = fit(orig.width, orig.height, width, height);
    if (resized.scale > 1) {
      return null;
    }
    return [
      newStruct('', {width: resized.width, height: resized.height}),
      newStruct('RemoteResource', {
        url: getURL('files/get_thumbnail', {
          path: input.id,
          format: 'jpeg',
          size: `w${width}h${height}`,
        }),
      }),
    ];
  });

  kv.push([
    newStruct('', {
      width: orig.width,
      height: orig.height,
    }),
    newStruct('RemoteResource', {
      url: getURL('files/download', {path: input.id}),
    }),
  ]);

  // $FlowIssue: Does not understand that filter removes all null values.
  return new Map(kv.filter(kv => kv));
}

function getURL(path: string, dbArgs: Object): string {
  const dbArgStr = encodeURIComponent(JSON.stringify(dbArgs));
  return `${contentHost}${path}?arg=${dbArgStr}&` +
      `authorization=Bearer%20${args['access-token']}`;
}

function newDate(iso: string): Struct {
  return newStruct('Date', {
    nsSinceEpoch: new Date(Date.parse(iso)).getTime() * nanosPerMilli,
  });
}
