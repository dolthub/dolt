// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {request} from 'http';
import {parse} from 'url';

export type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string},
  withCredentials? : boolean,
};

function fetch<T>(url: string, f: (buf: Buffer) => T, options: FetchOptions = {}): Promise<T> {
  const opts: any = parse(url);
  opts.method = options.method || 'GET';
  if (options.headers) {
    opts.headers = options.headers;
  }
  return new Promise((resolve, reject) => {
    const req = request(opts, res => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(res.statusCode);
        return;
      }
      let buf = new Buffer(0);
      res.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
      });
      res.on('end', () => {
        resolve(f(buf));
      });
    });
    req.on('error', err => {
      reject(err);
    });
    // Set an idle-timeout of 2 minutes. The contract requires us to manually abort the connection,
    // then catch that event and report an error.
    req.setTimeout(2 * 60 * 1000, () => req.abort());
    req.on('abort', () => {
      reject(new Error('HTTP request timed out'));
    });

    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

function bufferToArrayBuffer(buf: Buffer): ArrayBuffer {
  const ab = new ArrayBuffer(buf.length);
  const view = new Uint8Array(ab);
  for (let i = 0; i < buf.length; i++) {
    view[i] = buf[i];
  }
  return ab;
}

function arrayBufferToBuffer(ab: ArrayBuffer): Buffer {
  // $FlowIssue: Node type declaration doesn't include ArrayBuffer.
  return new Buffer(ab);
}

function bufferToString(buf: Buffer): string {
  return buf.toString();
}

function normalizeBody(opts: FetchOptions): FetchOptions {
  if (opts.body instanceof ArrayBuffer) {
    opts.body = arrayBufferToBuffer(opts.body);
  }
  return opts;
}

export function fetchText(url: string, options: FetchOptions = {}): Promise<string> {
  return fetch(url, bufferToString, normalizeBody(options));
}

export function fetchArrayBuffer(url: string, options: FetchOptions = {}): Promise<ArrayBuffer> {
  return fetch(url, bufferToArrayBuffer, options);
}
