// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

/* global Plotly */

import {
  DatasetSpec,
  Map,
} from '@attic/noms';

window.onload = main;
window.onresize = layout;

const data = {
  x: [],
  y: [],
  type: 'bar',
};

function main() {
  getMap().then(m => {
    m.forEach((v, k) => {
      data.x.push(k);
      data.y.push(v);
    })
    .then(layout);
  });
}

function layout() {
  Plotly.newPlot('myDiv', [data]);
}

function getMap() {
  const args = (() => {
    const s = location.search.substr(1);
    const m = {};
    for (const part of s.split('&')) {
      const [k, v] = part.split('=');
      m[k] = decodeURIComponent(v);
    }
    return m;
  })();

  if (args.ds) {
    const ds = DatasetSpec.parse(args.ds).dataset();
    return ds.headValue();
  } else {
    return Promise.resolve(new Map([['Donkeys', 36], ['Monkeys', 8], ['Giraffes', 12]]));
  }
}
