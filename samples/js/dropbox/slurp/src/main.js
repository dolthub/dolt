// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import argv from 'yargs';
import {
  default as fetch,
  Request,
} from 'node-fetch';
import {
  DatasetSpec,
  jsonToNoms,
  newStruct,
} from '@attic/noms';
import {stripIndent} from 'common-tags';

const apiHost = 'https://api.dropboxapi.com/2/';
const clearLine = '\x1b[2K\r';

const args = argv
  .usage(stripIndent`
    Downloads metadata from the Dropbox API

    Usage: node . --access-token=<token> <dest-dataset>

    To obtain a token, build <noms>/tools/oauthify, then run:

    ./oauthify --client-id=6i6tl9k390judrl --client-secret=bvmnnth5k2d2vc3 \\
    --auth-url=https://www.dropbox.com/oauth2/authorize \\
    --token-url=https://api.dropboxapi.com/oauth2/token --pass-client-in-url
    `)
  .demand(1)
  .option('access-token', {
    describe: 'Dropbox oauth access token',
    type: 'string',
    demand: true,
  })
  .option('path', {
    describe: 'Path to import, everything below will be snarfed',
    type: 'string',
  })
  .argv;

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

  const data = [];
  let count = 0;
  let resp = await callDropbox(apiHost + 'files/list_folder', {
    path: args['path'] || '',
    recursive: true,
    'include_media_info': true,
  });
  while (resp) {
    count += resp.entries.length;
    process.stdout.write(clearLine + `Slurped ${count} items...`);
    data.push(resp);
    if (resp.has_more) {
      resp = await callDropbox(apiHost + 'files/list_folder/continue', {
        cursor: resp.cursor,
      });
    } else {
      resp = null;
    }
  }

  await db.commit(out, jsonToNoms(data), {
    meta: newStruct('', {
      date: new Date().toISOString(),
    }),
  });
  await db.close();
  process.stdout.write(clearLine);
}

async function callDropbox(url: string, body: Object): Promise<any> {
  const params = {
    method: 'POST',
    headers: {
      'Authorization': '',
      'Content-type': 'application/json',
    },
    body: JSON.stringify(body),
  };

  if (args['access-token']) {
    params.headers['Authorization'] = `Bearer ${args['access-token']}`;
  }

  const q = new Request(url, params);
  const r = await fetch(q);
  if (r.status !== 200) {
    throw new Error(await r.text());
  }
  return await r.json();
}
