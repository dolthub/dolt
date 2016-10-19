// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  DatasetSpec,
  jsonToNoms,
  newStruct,
} from '@attic/noms';
import {
  default as fetch,
  Request,
  Headers,
} from 'node-fetch';
import type {
  Api,
  Albums,
  Faces,
  User,
} from './types.js';
import {getRefreshToken, getAccessTokenFromRefreshToken} from './oauth.js';

const usage = `Parses photo metadata out of Picasa API

Usage: either provide:
 1. an access token (these are short lived, and should only be used for testing),
    you can get these from https://developers.google.com/oauthplayground/
 2. a client ID and secret, with an optional refresh token

If 2 and if a refresh token isn't provided, one will be fetched, then picasa-slurp
must be re-run with that refresh token. Hold on to it.

In other words:
 node . --access-token <dest-dataset>
or
 node . --client-id=<id> --client-secret=<secret> [--refresh-token=<token> <dest-dataset>]`;

const args = argv
  .usage(usage)
  .option('access-token', {
    describe: 'OAuth2 access token (these are short lived and should only be used for testing)',
    type: 'string',
  })
  .option('client-id', {
    describe: 'Client ID for OAuth2 token request',
    type: 'string',
  })
  .option('client-secret', {
    describe: 'Client secret for OAuth2 token request',
    type: 'string',
  })
  .option('refresh-token', {
    describe: 'Picasa refresh token',
    type: 'string',
  })
  .option('parallelism', {
    alias: 'p',
    default: 10,
    describe: 'Number of parallel fetch requests',
    type: 'number',
  })
  .option('batch-size', {
    alias: 'b',
    default: 1000,
    describe: 'Number of images to fetch per request',
    type: 'number',
  })
  .argv;

const clearLine = '\x1b[2K\r';

let accessToken = '';

main()
  .then(() => process.exit(0))
  .catch(ex => {
    console.error(ex);
    process.exit(1);
  });

async function main(): Promise<void> {
  if (args['access-token']) {
    accessToken = args['access-token'];
  } else if (!args['client-id'] || !args['client-secret']) {
    throw usage;
  } else if (args['refresh-token']) {
    accessToken = await getAccessTokenFromRefreshToken(
        args['client-id'], args['client-secret'], args['refresh-token']);
    console.log(`Got access token ${accessToken}`);
  } else {
    const refreshToken = await getRefreshToken(args['client-id'], args['client-secret']);
    console.log(`\nGot refresh token ${refreshToken}. Run me again:`);
    console.log(` node .  --client-id=${args['client-id']} ` +
                `--client-secret=${args['client-secret']} ` +
                `--refresh-token ${refreshToken} <dest-dataset>`);
    return;
  }

  if (args._.length !== 1) {
    throw 'must provide output dataset';
  }

  const outSpec = DatasetSpec.parse(args._[0]);
  const [db, out] = outSpec.dataset();

  const user: User = await callPicasa('api/user/default?alt=json');
  if (!user.feed.entry) {
    console.log('no photos');
    return;
  }

  // Get album and face URL endpoints ahead of time so they can be requested in parallel batches.
  let numPhotosSum = 0;
  const albumURLs = [];
  const faceURLs = [];

  for (const entry of user.feed.entry) {
    const id = entry['gphoto$id']['$t'];
    const numPhotos = entry['gphoto$numphotos']['$t'];
    numPhotosSum += numPhotos;

    for (let i = 0; i < numPhotos; i += args['batch-size']) {
      // Note: start-index is (i + 1) because it's 1-based.
      // TODO: This is racy because photos may be added or removed between batch calls. Perhaps try
      // using published-min and published-max?
      const search = `?alt=json&max-results=${args['batch-size']}&start-index=${i + 1}&imgmax=d`;
      albumURLs.push(`api/user/default/albumid/${id}${search}`);
      faceURLs.push(`back_compat/user/default/albumid/${id}${search}&kind=photo&v=4&fd=shapes2`);
    }
  }

  let numPhotosSoFar = 0;
  const updateProgress = (responses: Albums[]) => {
    for (const r of responses) {
      numPhotosSoFar += r.feed.entry.length;
    }
    const percent = (100 * numPhotosSoFar / numPhotosSum).toFixed(2);
    process.stdout.write(clearLine + `${numPhotosSoFar}/${numPhotosSum} photos (${percent}%)`);
  };

  // Fetch faces and albums in parallel so that progress can be accurate.
  const [albumsArray, facesArray] = await Promise.all([
    callPicasaP(args.parallelism / 2, albumURLs, updateProgress),
    callPicasaP(args.parallelism / 2, faceURLs),
  ]);

  // Note: may be missing photos, see https://github.com/attic-labs/noms/issues/2698.
  console.log(`\nSlurped ${numPhotosSoFar} photos`);

  const albums = concatApiEntries((albumsArray: Albums[]));
  const faces = concatApiEntries((facesArray: Faces[]));

  // Note: albums and faces contain a lot of duplicate data. faces basically contains everything in
  // albums, plus face data. This is pretty wasteful and causes commit to take twice as long as
  // necessary. Consider doing something about that.

  const j = (v: any) => jsonToNoms(v);
  return db.commit(out, newStruct('', {
    albums: j(albums),
    faces: j(faces),
    user: j(user),
  }, {
    meta: {
      date: new Date().toISOString(),
    },
  })).then(() => db.close());
}

function callPicasa(path: string): Promise<any> {
  const url = 'https://picasaweb.google.com/data/feed/' + path;
  return fetch(new Request(url, {
    headers: new Headers({
      'Authorization': `Bearer ${accessToken}`,
    }),
  })).then(resp => {
    if (Math.floor(resp.status / 100) !== 2) {
      throw new Error(`${resp.status} ${resp.statusText}: ${url}`);
    }
    return resp.json();
  });
}

type ProgressFn = (more: any[]) => any;

async function callPicasaP(p: number, paths: string[], progFn: ProgressFn = () => undefined)
    : Promise<any[]> {
  const out = [];
  for (let i = 0; i < paths.length; i += p) {
    const more = await Promise.all(paths.slice(i, i + p).map(url => callPicasa(url)));
    progFn(more);
    out.push(...more);
  }
  return out;
}

function concatApiEntries<T>(api: Api<T>[]): T[] {
  const entries = api.map(a => a.feed.entry);
  return entries.length === 0 ? [] : entries[0].concat(...entries.slice(1));
}
