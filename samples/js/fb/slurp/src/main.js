// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  default as fetch,
  Request,
  Response,
  Headers,
} from 'node-fetch';
import {
  DatasetSpec,
  invariant,
  jsonToNoms,
  List,
  newStruct,
  Struct,
} from '@attic/noms';
import {stripIndent} from 'common-tags';

const graphAPIHost = 'https://graph.facebook.com/';

const args = argv
  .usage(stripIndent`
    Parses photo metadata from Facebook API

    Usage: node . --access-token=<token> [--exchange-token | <dest-dataset>]

    Create an access token as follows:
    1. Browse to https://developers.facebook.com/tools/explorer/
    2. Login with your Facebook credentials.
    3. Select "AtticIO Photo Importer" in the "Application" drop down.
    4. In the "Get Token" dropdown menu, select "Get User Access Token".
    5. Copy the Access Token from the textbox.
    6. (optional) Exchange the "short-lived" token from (5) for a long-lived one:
       node . --access-token=<short-lived-token> --exchange-token`)
  .option('access-token', {
    describe: 'Facebook API access key',
    type: 'string',
    demand: true,
  })
  .option('exchange-token', {
    describe: 'Exchange a short-lived token (~2 hours) for a long-lived one (~60 days)',
    type: 'boolean',
    demand: false,
  })
  .argv;

const clearLine = '\x1b[2K\r';

const query = [
  'place',
  'name',
  'backdated_time',
  'link',
  'name_tags',
  'updated_time',
  'created_time',
  'from',
  'id',
  'images',
  'width',
  'height',
  'likes.limit(1000){id,name}',
  'tags.limit(1000){x,y,id,name,tagging_user}',
];

main().catch(ex => {
  console.error(ex);
  process.exit(1);
});

async function main(): Promise<void> {
  if (args['exchange-token']) {
    const resp = await callFacebookRaw(graphAPIHost +
      'oauth/access_token?' +
      'grant_type=fb_exchange_token&' +
      'client_id=1025558197466738&' +
      'client_secret=45088a81dfea0faff8f91bbc6dde0a0c&' +
      'fb_exchange_token=' + args['access-token']);
    const body = await resp.text();
    if (resp.status !== 200) {
      throw `Error ${resp.status} ${resp.statusText}: ${body}`;
    }
    const t = body
      .split('&')
      .map(kv => kv.split('='))
      .filter(([k]) => k === 'access_token')
      .map(([, v]) => v)
      .shift();
    console.log('Long-lived access token: ' + t);
    return;
  }

  if (args._.length < 1) {
    throw 'required <dest-dataset> parameter not specified';
  }

  const outSpec = DatasetSpec.parse(args._[0]);
  if (!outSpec) {
    throw 'invalid destination dataset spec';
  }

  const [db, out] = outSpec.dataset();
  const [user, photos] = await Promise.all([
    getUser(),
    getPhotos(),
    // TODO: Add more object types here
  ]);
  await db.commit(out, newStruct('', {
    user,
    photos,
  }), {
    meta: newStruct('', {
      date: new Date().toISOString(),
    }),
  });
  process.stdout.write(clearLine);
  return;
}

async function getUser(): Promise<Struct> {
  const result = await jsonToNoms(await callFacebook(`${graphAPIHost}v2.8/me`));
  invariant(result instanceof Struct);
  return result;
}

async function getPhotos(): Promise<List<any>> {
  // Calculate the number of expected fetches via the list of albums, so that we can show progress.
  // This appears to be the fastest way (photos only let you paginate).
  // TODO: this falls down if you have more than 1k albums.
  const albumsJSON = await callFacebook(`${graphAPIHost}v2.8/me/albums?limit=1000&fields=count`);
  const expected = albumsJSON.data.reduce((prev, album) => prev + album.count, 0);
  let seen = 0;

  const updateProgress = () => {
    const progress = Math.round(seen / expected * 100);
    process.stdout.write(clearLine + `${progress}% photos of ${expected} imported...`);
  };

  updateProgress();

  // Sadly we cannot issue these requests in parallel because Facebook doesn't give you a way to
  // get individual page URLs ahead of time.
  // Note: Even though the documentation says that the max value for 'limit' is 1000, aa@ observed
  // errors from fb servers past about 500.
  let result = await new List();
  let url = `${graphAPIHost}v2.8/me/photos/uploaded?limit=500&date_format=U&fields=` +
      `${query.join(',')}`;
  while (url) {
    const photosJSON = await callFacebook(url);
    result = await result.append(await jsonToNoms(photosJSON));
    if (photosJSON.paging !== undefined) {
      url = photosJSON.paging.next;
    } else {
      url = null;
    }
    if (photosJSON.data !== undefined) {
      seen += photosJSON.data.length;
    }
    updateProgress();
  }
  return result;
}

function callFacebook(path: string): Promise<any> {
  return callFacebookRaw(path).then(r => r.json());
}

function callFacebookRaw(url: string): Promise<Response> {
  return fetch(new Request(url, {
    headers: new Headers(
      {'Authorization': `Bearer ${args['access-token']}`}),
  }));
}
