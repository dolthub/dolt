// @flow

type FetchOptions = {
  method?: string,
  body?: any,
  headers?: {[key: string]: string}
};

function fetch<T>(url: string, responseType: string, options: FetchOptions = {}): Promise<T> {
  const xhr = new XMLHttpRequest();
  xhr.responseType = responseType;
  const method = options.method || 'GET';
  xhr.open(method, url, true);
  const p = new Promise((res, rej) => {
    xhr.onloadend = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        res(xhr.response);
      } else {
        rej(xhr.status);
      }
    };
  });
  if (options.headers) {
    for (const key in options.headers) {
      xhr.setRequestHeader(key, options.headers[key]);
    }
  }
  xhr.send(options.body);
  return p;
}

export function fetchText(url: string, options: FetchOptions = {}): Promise<string> {
  return fetch(url, 'text', options);
}

export function fetchArrayBuffer(url: string, options: FetchOptions = {}): Promise<ArrayBuffer> {
  return fetch(url, 'arraybuffer', options);
}
