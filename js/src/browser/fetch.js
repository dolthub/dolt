// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0


export type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string},
  respHeaders?: string[],
  withCredentials? : boolean,
};

type TextResponse = {headers: Map<string, string>, buf: string}
type BufResponse = {headers: Map<string, string>, buf: Uint8Array}
type Response<T> = {headers: Map<string, string>, buf: T}

function fetch<T>(url: string, responseType: string, options: FetchOptions = {}):
 Promise<Response<T>> {
  const xhr = new XMLHttpRequest();
  xhr.responseType = responseType;
  const method = options.method || 'GET';
  xhr.open(method, url, true);
  const p = new Promise((res, rej) => {
    xhr.onloadend = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        const headers = new Map();
        if (options.respHeaders) {
          for (const header of options.respHeaders) {
            headers.set(header, res.headers[header]);
          }
        }
        res({headers: headers, buf: xhr.response});
      } else {
        rej(xhr.status);
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
      .then(resp => ({headers: new Map(resp.headers), buf: resp.text()}));
  }

  return fetch(url, 'text', options);
}

export function fetchUint8Array(url: string, options: FetchOptions = {}): Promise<BufResponse> {
  if (self.fetch) {
    return self.fetch(url, options)
      .then(resp => [resp.headers, resp.arrayBuffer()])
      .then((headers, ar) => ({headers: headers, buf: new Uint8Array(ar)}));
  }

  return fetch(url, 'arraybuffer', options);
}
