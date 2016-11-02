// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// Only encode the URI components that will break the URL. Characters like [] will be %-encoded by
// default, but they don't need to be.
const encodePatterns = [
  [/%/g, '%25'],
  [/&/g, '%26'],
  [/=/g, '%3D'],
];

/**
 * Returns a map of URL param key to value.
 */
export function searchToParams(search: string): Map<string, string> {
  // Note: this way anything after the # will end up in `params`, which is what we want.
  const params = new Map();
  const paramsIdx = search.indexOf('?');
  if (paramsIdx > -1) {
    decodeURIComponent(search.slice(paramsIdx + 1)).split('&').forEach(pair => {
      const idx = pair.indexOf('=');
      params.set(pair.slice(0, idx), pair.slice(idx + 1));
    });
  }
  return params;
}

/**
 * Returns the search location string representation of a param map.
 */
export function paramsToSearch(params: Map<string, string>): string {
  const encode = s => {
    for (const [p, r] of encodePatterns) {
      s = s.replace(p, r);
    }
    return s;
  };

  let search = '';
  for (const [k, v] of params) {
    search += search === '' ? '?' : '&';
    search += encode(k) + '=' + encode(v);
  }
  return search;
}
