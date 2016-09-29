// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import {
  default as fetch,
  Request,
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

const args = argv
  .usage(
    'Parses photo metadata from Facebook API\n\n' +
    'Usage: node . --access-token=<token> <dest-dataset>\n\n' +
    'Create an access token as follows:\n' +
    '1. Browse to https://developers.facebook.com/tools/explorer/\n' +
    '2. Login with your Facebook credentials\n' +
    '3. In the "Get Token" dropdown menu, select "Get User Access Token"\n' +
    '4. Copy the Access Token from the textbox')
  .demand(1)
  .option('access-token', {
    describe: 'Facebook API access key',
    type: 'string',
    demand: true,
  })
  .argv;

const clearLine = '\x1b[2K\r';

const query = [
  'place',
  'name',
  'backdated_time',
  'last_used_time',
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
  await db.commit(out, newStruct('', {user, photos}));
  process.stdout.write(clearLine);
  return;
}

async function getUser(): Promise<Struct> {
  const result = await jsonToNoms(await callFacebook('v2.7/me'));
  invariant(result instanceof Struct);
  return result;
}

async function getPhotos(): Promise<List<any>> {
  // Calculate the number of expected fetches via the list of albums, so that we can show progress.
  // This appears to be the fastest way (photos only let you paginate).
  const batchSize = 1000;
  const albumsJSON = await callFacebook(`v2.5/me/albums?limit=${batchSize}&fields=count`);
  const expected = albumsJSON.data.reduce((prev, album) => prev + album.count, 0);
  let seen = 0;

  const updateProgress = () => {
    const progress = Math.round(seen / expected * 100);
    process.stdout.write(clearLine + `${progress}% photos of ${expected} imported...`);
  };

  updateProgress();

  // Sadly we cannot issue these requests in parallel because Facebook doesn't give you a way to
  // get individual page URLs ahead of time.
  let result = await new List();
  let url = 'v2.7/me/photos/uploaded?limit=1000&fields=' + query.join(',') +
      '&date_format=U';
  while (url) {
    const photosJSON = await callFacebook(url);
    result = await result.append(await jsonToNoms(photosJSON));
    url = photosJSON.paging.next;
    seen += photosJSON.data.length;
    updateProgress();
  }
  return result;
}

function callFacebook(path: string): Promise<any> {
  const url = 'https://graph.facebook.com/' + path;
  return fetch(new Request(url, {
    headers: new Headers(
      {'Authorization': `Bearer ${args['access-token']}`}),
  })).then(resp => resp.json());
}
