// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0


export type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string},
  withCredentials? : boolean,
};

type Response<T> = {headers: {[key: string]: string}, buf: T};
type TextResponse = Response<string>;
type BufResponse = Response<Uint8Array>;

function fetch<T>(url: string, responseType: string, options: FetchOptions = {}):
 Promise<Response<T>> {
  const xhr = new XMLHttpRequest();
  xhr.responseType = responseType;
  const method = options.method || 'GET';
  xhr.open(method, url, true);
  const p = new Promise((res, rej) => {
    xhr.onloadend = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        res({headers: res.headers, buf: xhr.response});
      } else {
        rej(new Error(`HTTP Error: ${xhr.status}`));
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
      .then(resp => ({headers: resp.headers, buf: resp.text()}));
  }

  return fetch(url, 'text', options);
}

export function fetchUint8Array(url: string, options: FetchOptions = {}): Promise<BufResponse> {
  if (self.fetch) {
    return self.fetch(url, options)
      .then(resp => ({headers: resp.headers, buf: new Uint8Array(resp.arrayBuffer())}));
  }

  return fetch(url, 'arraybuffer', options);
}
