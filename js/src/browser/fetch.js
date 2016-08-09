// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {notNull} from '../assert.js';

export type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string},
  withCredentials? : boolean,
};

type Response<T> = {headers: Map<string, string>, buf: T};
type TextResponse = Response<string>;
type BufResponse = Response<Uint8Array>;

function fetch<T>(url: string, responseType: string, options: FetchOptions = {}):
 Promise<Response<T>> {
  const xhr = new XMLHttpRequest();
  xhr.responseType = responseType;
  const method = options.method || 'GET';
  xhr.open(method, url, true);
  const p = new Promise((resolve, reject) => {
    xhr.onloadend = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        // Workaround Flow
        let buf: any = xhr.response;
        if (responseType === 'arraybuffer') {
          buf = new Uint8Array(buf);
        }
        resolve({headers: makeHeaders(xhr), buf});
      } else {
        reject(new Error(`HTTP Error: ${xhr.status}`));
      }
    };
  });
  if (options.withCredentials) {
    xhr.withCredentials = true;
  }
  if (options.headers) {
    for (const key in options.headers) {
      xhr.setRequestHeader(key, options.headers[key]);
    }
  }
  xhr.send(options.body);
  return p;
}

export function fetchText(url: string, options: FetchOptions = {}): Promise<TextResponse> {
  if (self.fetch) {
    return self.fetch(url, options)
      // resp.headers is a Headers which is a multi map, which is similar enough for now.
      .then(resp => {
        const {headers} = resp;
        return resp.text().then(text => ({headers, buf: text}));
      });
  }

  return fetch(url, 'text', options);
}

export function fetchUint8Array(url: string, options: FetchOptions = {}): Promise<BufResponse> {
  if (self.fetch) {
    return self.fetch(url, options)
      // resp.headers is a Headers which is a multi map, which is similar enough for now.
      .then(resp => {
        const {headers} = resp;
        return resp.arrayBuffer().then(buf => ({headers, buf: new Uint8Array(buf)}));
      });
  }

  return fetch(url, 'arraybuffer', options);
}

function makeHeaders(xhr: XMLHttpRequest): Map<string, string> {
  const m = new Map();
  const headers = xhr.getAllResponseHeaders().split(/\r\n/);  // spec requires \r\n
  for (const header of headers) {
    if (header) {
      const [, name, value] = notNull(header.match(/([^:]+): (.*)/));
      m.set(name.toLowerCase(), value);
    }
  }
  return m;
}
