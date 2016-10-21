// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow
import {OAuth2} from 'oauth';
import readline from 'readline';

const scope = 'https://picasaweb.google.com/data';
const authPath = 'https://accounts.google.com/o/oauth2/auth';
const tokenPath = 'https://accounts.google.com/o/oauth2/token';

// This magic URL causes Google's oauth endpoint to print out the token for the user to copy,
// rather than actually redirecting to it.
const redirectUri = 'urn:ietf:wg:oauth:2.0:oob';

/**
 * Gets a refresh token for the Picasa API. Refresh tokens are used to issue access tokens.
 */
export function getRefreshToken(clientId: string, clientSecret: string): Promise<string> {
  return getAuthCode(clientId, clientSecret)
    .then(authCode => new Promise((res, rej) => {
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

function getAuthCode(clientId: string, clientSecret: string)
    : Promise<string> {
  return new Promise(res => {
    const secret = String(Math.random());
    const oauth2 = newOAuth2(clientId, clientSecret);
    const authUrl = oauth2.getAuthorizeUrl({
      'access_type': 'offline', // without this, we won't be issued a refresh token
      'redirect_uri': redirectUri,
      'response_type': 'code',
      scope,
      state: secret,
    });

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    rl.question(
      `Visit the following URL and paste the code you get:\n\n\t${authUrl}\n\n`,
      answer => {
        res(answer);
        rl.close();
      });
  });
}

function newOAuth2(clientId: string, clientSecret: string): OAuth2 {
  return new OAuth2(clientId, clientSecret, '', authPath, tokenPath, null);
}
