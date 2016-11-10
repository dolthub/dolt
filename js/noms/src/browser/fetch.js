// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {notNull} from '../assert.js';
import HttpError from '../http-error.js';

export type FetchOptions = {
  method?: ?MethodType, // from flowlib bom.js
  body?: any,
  headers?: ?{[key: string]: string},
  withCredentials?: ?boolean,
};

type ResponseType<T> = {headers: Map<string, string>, buf: T};
type TextResponse = ResponseType<string>;
type BufResponse = ResponseType<Uint8Array>;

function internalFetch<T>(url: string, responseType: string, options: FetchOptions = {})
    : Promise<ResponseType<T>> {
  const xhr = new XMLHttpRequest();
  xhr.responseType = responseType;
  const method = options.method || 'GET';
  xhr.open(method, url, true);
  const p = new Promise((resolve, reject) => {
    // React Native does not support loadend events.
    // https://github.com/facebook/react-native/pull/10047
    xhr.onreadystatechange = () => {
      if (xhr.readyState !== xhr.DONE) {
        return;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        // Workaround Flow
        let buf: any = xhr.response;
        if (responseType === 'arraybuffer') {
          buf = new Uint8Array(buf);
        }
        resolve({headers: makeHeaders(xhr), buf});
      } else {
        reject(new HttpError(xhr.status));
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

function hasWorkingFetch() {
  return typeof fetch === 'function' && typeof Response === 'function' &&
    typeof Response.prototype.arrayBuffer === 'function';
}

export function fetchText(url: string, options: FetchOptions = {}): Promise<TextResponse> {
  if (hasWorkingFetch()) {
    // $FlowIssue: Any object should work as RequestOptions
    const o: RequestOptions = options;
    return fetch(url, o)
      // resp.headers is a Headers which is a multi map, which is similar enough for now.
      .then(resp => {
        // Work around. Headers is a Map like object.
        const headers: any = resp.headers;
        return resp.text().then(text => ({headers, buf: text}));
      });
  }

  return internalFetch(url, 'text', options);
}

export function fetchUint8Array(url: string, options: FetchOptions = {}): Promise<BufResponse> {
  if (hasWorkingFetch()) {
    // $FlowIssue: Any object should work as RequestOptions
    const o: RequestOptions = options;
    return fetch(url, o)
      // resp.headers is a Headers which is a multi map, which is similar enough for now.
      .then(resp => {
        // Work around. Headers is a Map like object.
        const headers: any = resp.headers;
        return resp.arrayBuffer().then(buf => ({headers, buf: new Uint8Array(buf)}));
      });
  }

  return internalFetch(url, 'arraybuffer', options);
}

function makeHeaders(xhr: XMLHttpRequest): Map<string, string> {
  const m = new Map();
  // React Native uses a JS implementation that combines the headers using \n only.
  // https://github.com/facebook/react-native/pull/10034
  const headers = xhr.getAllResponseHeaders().split(/\r?\n/);  // spec requires \r\n
  for (const header of headers) {
    if (header) {
      const [, name, value] = notNull(header.match(/([^:]+): (.*)/));
      m.set(name.toLowerCase(), value);
    }
  }
  return m;
}
