// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import fit from 'aspect-fit';
import {
  DatasetSpec,
  getTypeOfValue,
  isSubtype,
  makeListType,
  makeStructType,
  Map,
  newStruct,
  numberType,
  PathSpec,
  Set,
  stringType,
  Struct,
  walk,
} from '@attic/noms';

const args = argv
  .usage(
    'Finds Noms Photo objects from output of picasa/slurp\n\n' +
    'Usage: node . [flags] <in-path> <out-dataset>')
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

const stringStruct = makeStructType('', {
  Q24t: stringType,
});

const sourceType = makeStructType('', {
  mediaQ24group: makeStructType('', {
    mediaQ24thumbnail: makeListType(makeStructType('', {
      height: numberType,
      width: numberType,
      url: stringType,
    })),
  }),
  gphotoQ24id: stringStruct,
  gphotoQ24width: stringStruct,
  gphotoQ24height: stringStruct,
  published: stringStruct,
  updated: stringStruct,
});

const hasTitle = makeStructType('', {
  title: stringStruct,
});

const hasTimestamp = makeStructType('', {
  gphotoQ24timestamp: stringStruct,
});

const hasFaces = makeStructType('', {
  gphotoQ24shapes: makeStructType('', {
    gphotoQ24shape: makeListType(makeStructType('', {
      name: stringType,
      upperLeft: stringType,
      lowerRight: stringType,
    })),
  }),
});

const hasGeo = makeStructType('', {
  georssQ24where: makeStructType('', {
    gmlQ24Point: makeStructType('', {
      gmlQ24pos: stringStruct,
    }),
  }),
});

// From: https://developers.google.com/picasa-web/docs/2.0/reference#Parameters
// More sizes are available (in fact, it appears you can just put any size you want in the URL),
// but these suffice for us.
const sizes = [320, 640, 1024, 1440, 1600];

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
  const result = [];

  await walk(input, db, async (v: any) => {
    if (v instanceof Struct && isSubtype(sourceType, getTypeOfValue(v))) {
      const w = parseInt(v.gphotoQ24width.Q24t, 10);
      const h = parseInt(v.gphotoQ24height.Q24t, 10);

      const resources = await getResources(v, w, h);
      const photo: Object = {
        id: 'https://github.com/attic-labs/noms/samples/js/picasa/find-photos' +
            `#${v.gphotoQ24id.Q24t}`,
        datePublished: getDate(Date.parse(v.published.Q24t)),
        dateUpdated: getDate(Date.parse(v.updated.Q24t)),
        resources: resources,
        sizes: await getSizes(resources),
        sources: sourceTags,
      };

      if (isSubtype(hasTitle, v.type)) {
        photo.title = v.title.Q24t;
      }

      if (isSubtype(hasTimestamp, v.type)) {
        photo.dateTaken = getDate(parseInt(
          v.gphotoQ24timestamp.Q24t, 10));
      }

      if (isSubtype(hasGeo, v.type)) {
        const g = getGeo(v);
        if (g) {
          photo.geoposition = g;
        }
      }

      if (isSubtype(hasFaces, v.type)) {
        const f = await getFaces(v, w, h);
        if (f) {
          photo.faces = f;
        }
      }

      result.push(newStruct('Photo', photo));
      process.stdout.write(clearLine + `Indexed ${result.length} photos...`);
      return true;
    }
  });

  await outDB.commit(
    output,
    new Set(result),
    {
      meta: newStruct('', {
        date: new Date().toISOString(),
        input: pinnedSpec.toString(),
      }),
    }
  );
  await db.close();
  await outDB.close();

  process.stdout.write(clearLine);
}

function getGeo(input): ?Struct {
  const pattern = /^(\-?\d+\.\d+) (\-?\d+\.\d+)$/;
  const match = input.georssQ24where.gmlQ24Point.gmlQ24pos.Q24t.match(pattern);
  if (!match) {
    // This can happen even though we type-checked because the pattern might not match.
    return null;
  }

  return newStruct('Geoposition', {
    latitude: parseFloat(match[1]),
    longitude: parseFloat(match[2]),
  });
}

async function getSizes(resources: Map<Struct, Struct>)
    : Promise<Map<Struct, string>> {
  const tuples = [];
  await resources.forEach((v: any, k: Struct) => {
    tuples.push([k, v.url]);
  });
  return new Map(tuples);
}

async function getResources(input: Object, origWidth: number, origHeight: number)
    : Promise<Map<Struct, Struct>> {
  const thumbURL = (await input.mediaQ24group.mediaQ24thumbnail
    .get(0)).url.split('/');
  const sizePart = thumbURL.length - 2;

  const makeURL = s => {
    thumbURL[sizePart] = `s${s}`;
    return thumbURL.join('/');
  };

  const tuples = sizes.map(s => {
    const r = fit(origWidth, origHeight, s, s);
    if (r.scale > 1) {
      // $FlowIssue: Does not understand that filter removes all null values.
      return null;
    }
    return [
      newStruct('', {width: r.width, height: r.height}),
      newStruct('RemoteResource', {url: makeURL(s)}),
    ];
  }).filter(t => t);

  // The original file.
  tuples.push([
    newStruct('', {width: origWidth, height: origHeight}),
    newStruct('RemoteResource', {url: makeURL('d')}),
  ]);

  return new Map(tuples);
}

async function getFaces(photo: Object, origWidth: number, origHeight: number)
    : Promise<?Set<Struct>> {
  const parsePos = (s) => {
    const pattern = /^(\d+) (\d+)$/;
    const match = s.match(pattern);
    if (!match) {
      return null;
    }
    return [parseInt(match[1], 10), parseInt(match[2], 10)];
  };

  const result = [];
  await photo.gphotoQ24shapes.gphotoQ24shape.forEach(v => {
    const ul = parsePos(v.upperLeft);
    const lr = parsePos(v.lowerRight);
    if (!ul || !lr || !v.name) {
      return;
    }

    const [l, t] = ul;
    const [r, b] = lr;
    result.push(newStruct('', {
      name: v.name,
      x: l / origWidth,
      y: t / origHeight,
      w: (r - l) / origWidth,
      h: (b - t) / origHeight,
    }));
  });

  if (result.length === 0) {
    return null;
  }

  return new Set(result);
}

function getDate(ms: number) {
  return newStruct('Date', {
    nsSinceEpoch: ms * 1e6,
  });
}
