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
import Flickr from './flickr.js';

const args = argv
  .usage(
    'Parses photo metadata out of Flickr API\n\n' +
    'Usage: node . --api-key=<key> --api-secret=<secret> ' +
    '[--auth-token=<token> --auth-secret=<secret>] <dest-dataset>\n\n' +
    'You can create a Flickr API key at: ' +
    'https://www.flickr.com/services/apps/create/apply\n\n' +
    '--auth-token and --auth-secret are optional, but include them to avoid having\n' +
    'to reauth over and over if you are calling this repeatedly.')
  .demand(1)
  .option('api-key', {
    describe: 'Flickr API key',
    type: 'string',
    demand: true,
  })
  .option('api-secret', {
    description: 'Flickr API secret',
    type: 'string',
    demand: true,
  })
  .option('access-token', {
    description: 'Flickr access token',
    type: 'string',
  })
  .option('access-token-secret', {
    description: 'Flickr access token secret',
    type: 'string',
  })
  .argv;

const clearLine = '\x1b[2K\r';
const flickr = new Flickr(
  args['api-key'], args['api-secret'],
  args['access-token'], args['access-token-secret']);

let totalPhotos = 0;
let gottenPhotos = 0;

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

  if (!args['access-token'] || !args['access-token-secret']) {
    await flickr.authenticate();
    process.stdout.write(`Authenticated. Next time run:\n${
      process.argv.join(' ')} --access-token=${flickr.accessToken} --access-token-secret=${
      flickr.accessTokenSecret}\n\n`);
  }
  const userId = await getUserId();
  const photos = getAllPhotos(userId);
  const photosets = getPhotosets();

  await db.commit(out, jsonToNoms({
    photos: await photos,
    photosets: await photosets,
  }), {
    meta: newStruct('', {date: new Date().toISOString()}),
  });
  await db.close();
  process.stdout.write(clearLine);
}

function getUserId(): Promise<string> {
  return flickr.callApi('flickr.urls.getUserProfile').then(d => d.user.nsid);
}

function getAllPhotos(userId: string): Promise<Array<any>> {
  const minPrivacy = 1;
  const maxPrivacy = 5;
  const results = [];
  for (let i = minPrivacy; i <= maxPrivacy; i++) {
    results.push(getPhotos(userId, i));
  }
  return Promise.all(results);
}

async function getPhotos(userId: string, privacyFilter: number): Promise<Array<any>> {
  const perPage = 500;
  const p1 = await getPhotoPage(userId, privacyFilter, perPage, 1);
  totalPhotos += Number(p1.photos.total);
  updateStatus();
  const results = [];
  // We got page '1' (first page) above. Get pages 2-n here.
  for (let i = 2; i <= p1.photos.pages; i++) {
    results.push(getPhotoPage(userId, privacyFilter, perPage, i));
  }
  return [p1].concat(await Promise.all(results));
}

async function getPhotoPage(userId: string, privacyFilter: number, perPage: number,
    pageNumber: number): Promise<any> {
  const result = await flickr.callApi('flickr.photos.search', {
    'user_id': userId,
    'privacy_filter': String(privacyFilter),
    'per_page': String(perPage),
    page: String(pageNumber),
    extras: 'description,license,date_upload,date_taken,owner_name,icon_server,original_format,' +
        'last_update,geo,tags,machine_tags,o_dims,views,media,path_alias,url_sq,url_t,url_s,' +
        'url_q,url_m,url_n,url_z,url_c,url_l,url_o',
  });
  gottenPhotos += Number(result.photos.photo.length);
  updateStatus();
  return result;
}

async function getPhotosets(): Promise<any> {
  return await flickr.callApi('flickr.photosets.getList');
}

function updateStatus() {
  process.stdout.write(`${clearLine}${gottenPhotos}/${totalPhotos}...`);
}
