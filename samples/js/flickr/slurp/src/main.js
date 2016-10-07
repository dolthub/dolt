// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import {
  DatasetSpec,
  jsonToNoms,
  newStruct,
  Set,
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

  const flickr = new Flickr(args['api-key'], args['api-secret'],
                            args['access-token'], args['access-token-secret']);

  if (!args['access-token'] || !args['access-token-secret']) {
    await flickr.authenticate();
    process.stdout.write(`Authenticated. Next time run:\n${
      process.argv.join(' ')} --access-token=${flickr.accessToken} --access-token-secret=${
      flickr.accessTokenSecret}\n\n`);
  }

  const photosets = await getPhotosets(flickr);
  let seen = 0;
  const photosetsPromise = photosets.map(p => getPhotoset(flickr, p.id).then(p => {
    process.stdout.write(`${clearLine}${++seen} of ${photosets.length} photosets imported...`);
    return jsonToNoms(p);
  }));
  const setOfPhotosets = new Set(await Promise.all(photosetsPromise));

  process.stdout.write(clearLine);
  return db.commit(out, newStruct('', {
    photosetsMeta: jsonToNoms(photosets),
    photosets: await setOfPhotosets,
  }), {
    meta: newStruct('', {date: new Date().toISOString()}),
  })
  .then(() => db.close());
}

async function getPhotoset(flickr: Flickr, id: string): Promise<*> {
  const json = await flickr.callApi('flickr.photosets.getPhotos', {
    'photoset_id': id,
    extras: 'license, date_upload, date_taken, owner_name, icon_server, original_format, ' +
      'last_update, geo, tags, machine_tags, o_dims, views, media, path_alias, url_sq, url_t, ' +
      'url_s, url_m, url_o',
  });
  return json.photoset;
}

function getPhotosets(flickr: Flickr): Promise<*> {
  return flickr.callApi('flickr.photosets.getList').then(v => v.photosets.photoset);
}
