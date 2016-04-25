/* global Plotly */

import {
  boolType,
  Kind,
  makeCompoundType,
  newMap,
  stringType,
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
  return newMap(['Donkeys', 36, 'Monkeys', 8, 'Giraffes', 12], makeMapType(stringType, boolType));
}
