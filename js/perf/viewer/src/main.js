// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {
  Dataset,
  DatasetSpec,
  getHashOfValue,
  invariant,
  List,
  Map as NomsMap,
  Struct,
} from '@attic/noms';
import type {Value} from '@attic/noms';

declare class Chart {
}

window.onload = load;
window.onpopstate = load;
window.onresize = render;

// The maximum number of git revisions to show in the perf history.
// The larger this number, the more screen real estate needed to render the graph - and the slower
// it will take to render, since the entire parent commit chain must be walked to form the graph.
// TODO: Implement paging mechanism.
const MAX_PERF_HISTORY = 15;

let chartDatasets: Map<string /* test name */, (number | null)[] /* elapsed, in seconds */>;
let chartLabels: string[];

async function load() {
  const params = getParams();
  if (!params.ds) {
    alert('Must provide a ?ds= param');
    return;
  }

  if (params.refresh) {
    // TODO: Poll Noms then refresh the graph, instead of reloading whole page.
    setTimeout(() => location.reload(), Number(params.refresh));
  }

  const dsSpec = DatasetSpec.parse(params.ds);
  const [perfData, gitRevs] = await getPerfHistory(dsSpec.dataset());

  chartDatasets = new Map();
  chartLabels = gitRevs.map(rev => rev.slice(0, 6));

  // Each Noms commit might have a different set of tests (e.g. tests may have been added or removed
  // between git revisions), but they should all go on the graph. Find every test up-front.
  const firstReps = await Promise.all(perfData.map(pd => {
    invariant(pd.reps instanceof List);
    return pd.reps.get(0);
  }));
  const testNamesSet = new Set();
  for (const fr of firstReps) {
    (await keys(fr)).forEach(testName => testNamesSet.add(testName));
  }
  const testNames = Array.from(testNamesSet);

  const getElapsed = async (testName: string, pd: Struct) => {
    invariant(pd.reps instanceof List);
    const reps = await pd.reps.toJS();
    const elapsedOrNulls = await Promise.all(reps.map(rep => {
      invariant(rep instanceof NomsMap);
      // Note: despite how this code is structured, either all reps should have test data for this
      // value, or none should. Ideally we'd be able to bail at this point.
      return rep.get(testName).then(d => d ? d.elapsed / 1e9 : null);
    }));
    return elapsedOrNulls[0] !== null ? median(elapsedOrNulls) : null;
  };

  const getChartData = (testName: string) =>
    Promise.all(perfData.map(pd => getElapsed(testName, pd)));

  // TODO: Scale the data to "max while < 1000" so that these all fit on the same graph (not 1e9)?
  const testChartData = await Promise.all(testNames.map(getChartData));
  for (let i = 0; i < testNames.length; i++) {
    chartDatasets.set(testNames[i], testChartData[i]);
  }

  render();
}

// Returns the history of perf data with their git revisions, from oldest to newest.
async function getPerfHistory(ds: Dataset): Promise<[Struct[], string[]]> {
  const perfData = [], gitRevs = [];

  for (let head = await ds.head(), i = 0; head && i < MAX_PERF_HISTORY; i++) {
    const val = head.value;
    invariant(val instanceof Struct);
    perfData.push(val);
    gitRevs.push(val.nomsRevision);

    const parentRef = await head.parents.first(); // TODO: how to deal with multiple parents?
    head = parentRef ? await parentRef.targetValue(ds.database) : null;
  }

  return [perfData, gitRevs];
}

// Returns a map of URL param key to value.
function getParams(): {[key: string]: string} {
  // Note: this way anything after the # will end up in `params`, which is what we want.
  const params = {};
  const paramsIdx = location.href.indexOf('?');
  if (paramsIdx > -1) {
    decodeURIComponent(location.href.slice(paramsIdx + 1)).split('&').forEach(pair => {
      const [k, v] = pair.split('=');
      params[k] = v;
    });
  }
  return params;
}

async function render() {
  if (!chartDatasets) {
    return;
  }

  const datasets = [];
  for (const [testName, elapsed] of chartDatasets) {
    const [borderColor, backgroundColor] = await getSolidAndAlphaColors(testName);
    datasets.push({
      backgroundColor,
      borderColor,
      borderWidth: 1,
      data: elapsed,
      label: testName,
    });
  }

  new Chart(document.getElementById('chart'), {
    type: 'line',
    data: {
      labels: chartLabels,
      datasets,
    },
    options: {
      scales: {
        yAxes: [{
          scaleLabel: {
            display: true,
            labelString: 'elapsed (seconds)',
          },
          ticks: {
            beginAtZero: true,
          },
        }],
        xAxes: [{
          scaleLabel: {
            display: true,
            labelString: 'github.com/attic-labs/noms git revision',
          },
        }],
      },
    },
  });
}

// Returns the median of numbers in `nums`.
function median(nums: number[]): number {
  const sorted = nums.slice();
  sorted.sort();
  const lenDiv2 = Math.floor(nums.length / 2);
  let res = nums[lenDiv2];
  if (nums.length % 2 === 0) {
    res += nums[lenDiv2 - 1];
    res /= 2;
  }
  return res;
}

// Generates a light and dark version of some color randomly (but stable) derived from `str`.
async function getSolidAndAlphaColors(str: string): Promise<[string, string]> {
  // getHashOfValue() returns a Uint8Array, so pull out the first 3 8-bit numbers - which will be in
  // the range [0, 255] - to generate a full RGB colour.
  const [r, g, b] = getHashOfValue(str).digest;
  return [`rgb(${r}, ${g}, ${b})`, `rgba(${r}, ${g}, ${b}, 0.25)`];
}

// Returns the keys of `map`.
function keys<K: Value, V: Value>(map: NomsMap<K, V>): Promise<K[]> {
  const keys = [];
  return map.forEach((_, key) => {
    keys.push(key);
  }).then(() => keys);
}
