// @flow

import {request} from 'http';
import {parse} from 'url';

type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string}
};

function fetch<T>(url: string, f: (buf: Buffer) => T, options: FetchOptions = {}): Promise<T> {
  let opts: any = parse(url);
  opts.method = options.method || 'GET';
  if (options.headers) {
    opts.headers = options.headers;
  }
  return new Promise((resolve, reject) => {
    let req = request(opts, res => {
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
    if (options.body) {
      req.write(options.body);
    }
    req.end();
  });
}

function bufferToArrayBuffer(buf: Buffer): ArrayBuffer {
  let ab = new ArrayBuffer(buf.length);
  let view = new Uint8Array(ab);
  for (let i = 0; i < buf.length; i++) {
    view[i] = buf[i];
  }
  return ab;
}


function bufferToString(buf: Buffer): string {
  return buf.toString();
}

export function fetchText(url: string, options: FetchOptions = {}): Promise<string> {
  return fetch(url, bufferToString, options);
}

export function fetchArrayBuffer(url: string, options: FetchOptions = {}): Promise<ArrayBuffer> {
  return fetch(url, bufferToArrayBuffer, options);
}
