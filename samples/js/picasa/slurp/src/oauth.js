// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {createServer as createHttpServer} from 'http';
import {OAuth2} from 'oauth';
import {parse as parseQueryString} from 'querystring';
import {parse as parseUrl} from 'url';

const scope = 'https://picasaweb.google.com/data';
const authPath = 'https://accounts.google.com/o/oauth2/auth';
const tokenPath = 'https://accounts.google.com/o/oauth2/token';

/**
 * Gets a refresh token for the Picasa API. Refresh tokens are used to issue access tokens.
 */
export function getRefreshToken(clientId: string, clientSecret: string): Promise<string> {
  return getAuthCodeViaURL(clientId, clientSecret)
    .then(([authCode, redirectUri]) => new Promise((res, rej) => {
      const oauth = newOAuth2(clientId, clientSecret);
      oauth.getOAuthAccessToken(authCode, {
        'grant_type': 'authorization_code',
        'redirect_uri': redirectUri,
        scope,
      }, (error: string, _, refreshToken: string) => {
        if (error) {
          rej(error);
        } else {
          res(refreshToken);
        }
      });
    }));
}

/**
 * Gets an access token from a request token for the Picasa API. Access tokens are used for
 * authenticated API calls.
 */
export function getAccessTokenFromRefreshToken(
    clientId: string, clientSecret: string, refreshToken: string): Promise<string> {
  return new Promise((res, rej) => {
    const oauth = newOAuth2(clientId, clientSecret);
    oauth.getOAuthAccessToken(refreshToken, {
      'grant_type': 'refresh_token',
    }, (error: string, accessToken: string) => {
      if (error) {
        rej(error);
      } else {
        res(accessToken);
      }
    });
  });
}

function getAuthCodeViaURL(clientId: string, clientSecret: string)
    : Promise<[string /* auth code */, string /* authorize URL */]> {
  return new Promise((res, rej) => {
    // To be an OAuth endpoint, host an HTTP server on a random port to serve as the redirect URL,
    // then capture the access code.
    const server = createHttpServer((request, response) => {
      let code = null;
      try {
        const url = parseUrl(request.url);
        if (!url.query) {
          rej(`oauth response ${request.url} missing query string`);
          return;
        }

        const qs = parseQueryString(url.query);
        if (qs.state !== secret) {
          rej(`invalid secret ${qs.state}, expected ${secret}`);
        } else if (!qs.code) {
          rej(`oauth response ${request.url} does not have a code`);
        } else {
          code = qs.code;
          res([code, redirectUri]);
        }
      } finally {
        const message = code ? `got code ${code}` : 'failed to get code';
        response.end(`<body>${message}</body>`);
      }
    }).listen(0);

    const secret = String(Math.random());
    const redirectUri = `http://localhost:${server.address().port}`;
    const oauth2 = newOAuth2(clientId, clientSecret);
    const authUrl = oauth2.getAuthorizeUrl({
      'access_type': 'offline', // without this, we won't be issued a refresh token
      'redirect_uri': redirectUri,
      'response_type': 'code',
      scope,
      state: secret,
    });
    console.log(authUrl);
  });
}

function newOAuth2(clientId: string, clientSecret: string): OAuth2 {
  return new OAuth2(clientId, clientSecret, '', authPath, tokenPath, null);
}
