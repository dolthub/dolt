// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

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
  static defaults: {
    global: {
      defaultFontColor: string;
    };
  };
}

type DataPoint = {
  median: number;
  stddev: number;
};

type DataPoints = Map<string /* test name */, (DataPoint | null)[]>;

window.onload = load;

// The maximum number of git revisions to show in the perf history.
//
// The larger this number, the more screen real estate needed to render the graph - and the slower
// it will take to render, since the entire parent commit chain must be walked to form the graph.
// TODO: Implement paging mechanism.
const MAX_PERF_HISTORY = 20;

// The frequency in ms to refresh the page, which will cycle through the datasets. Can be overridden
// with the 'refresh' URL parameter.
const DEFAULT_REFRESH_MS = 60 * 1000;

function load() {
  const params = getParams();
  if (!params.ds) {
    alert('Must provide a ?ds= param');
    return;
  }

  const datasets = params.ds.split(',');
  const refresh = 'refresh' in params ? Number(params.refresh) : DEFAULT_REFRESH_MS;

  let datasetIdx = 0;
  setInterval(() => {
    datasetIdx = (datasetIdx + 1) % datasets.length;
    loadDataset(datasets[datasetIdx]);
  }, refresh);

  loadDataset(datasets[0]);
}

async function loadDataset(dsID: string) {
  const dsSpec = DatasetSpec.parse(dsID);
  const [, ds] = dsSpec.dataset();
  const [perfData, gitRevs] = await getPerfHistory(ds);

  // git describe --always uses the first 7 characters.
  const labels = gitRevs.map(rev => rev.slice(0, 7));
  const datapoints: DataPoints = new Map();

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

  const getDataForTest = async (testName: string, pd: Struct) => {
    invariant(pd.reps instanceof List);
    const reps = await pd.reps.toJS();
    const elapsedOrNulls = await Promise.all(reps.map(rep => {
      invariant(rep instanceof NomsMap);
      // Note: despite how this code is structured, either all reps should have test data for this
      // value, or none should. Ideally we'd be able to bail at this point.
      return rep.get(testName).then(d => d ? d.elapsed / 1e9 : null);
    }));
    return elapsedOrNulls[0] !== null ? makeDataPoint(elapsedOrNulls) : null;
  };

  const getChartData = (testName: string) =>
    Promise.all(perfData.map(pd => getDataForTest(testName, pd)));

  // TODO: Scale the data to "max while < 1000" so that these all fit on the same graph (not 1e9)?
  const testChartData = await Promise.all(testNames.map(getChartData));
  for (let i = 0; i < testNames.length; i++) {
    datapoints.set(testNames[i], testChartData[i]);
  }

  render(dsID, labels, datapoints);
}

// Returns the history of perf data with their git revisions, from oldest to newest.
async function getPerfHistory(ds: Dataset): Promise<[Struct[], string[]]> {
  const perfData = [], gitRevs = [];

  for (let head = await ds.head(), i = 0; head && i < MAX_PERF_HISTORY; i++) {
    const val = head.value;
    invariant(val instanceof Struct);
    perfData.unshift(val);
    gitRevs.unshift(val.nomsRevision);

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

async function render(ds: string, labels: string[], datapoints: DataPoints) {
  // We use the point radius to indicate the standard deviation, for lack of any better option.
  // Unfortunately chart.js doesn't provide any way to scale this relative to large the Y axis
  // values are with respect to the graph pixel height.
  //
  // So, try to approximate it by taking into account: (a) the expected magnitude of the Y axis (the
  // maximum value), and (b) how much space the graph will take up on the screen (half of screen
  // *width* - this does appear to be what chart.js does).
  const maxElapsedTime = Array.from(datapoints.values()).reduce((max, dataPoints) => {
    const medians = dataPoints.map(dp => dp !== null ? dp.median : 0);
    return Math.max(max, ...medians);
  }, 0);
  const graphHeight = document.body.scrollWidth / 2;
  const getStddevPointRadius = stddev => Math.ceil(stddev / maxElapsedTime * graphHeight);

  const datasets = [];
  for (const [testName, dataPoints] of datapoints) {
    const [borderColor, backgroundColor] = getSolidAndAlphaColors(testName);
    datasets.push({
      backgroundColor,
      borderColor,
      borderWidth: 1,
      pointRadius: dataPoints.map(dp => dp !== null ? getStddevPointRadius(dp.stddev) : 0),
      data: dataPoints.map(dp => dp !== null ? dp.median : null),
      label: testName,
      _maxMedian: Math.max(...dataPoints.map(dp => dp !== null ? dp.median : 0)), // for our sorting
    });
  }

  // Draw the datasets in order of largest to smallest, so that we (try not to) draw over the top of
  // entire datasets.
  datasets.sort((a, b) => a._maxMedian - b._maxMedian);

  Chart.defaults.global.defaultFontColor = 'white';
  new Chart(document.getElementById('chart'), {
    data: {
      labels,
      datasets,
    },
    options: {
      scales: {
        yAxes: [{
          scaleLabel: {
            display: true,
            labelString: 'elapsed seconds (point radius is standard deviation to scale)',
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
      title: {
        display: true,
        text: ds,
      },
    },
    type: 'line',
  });
}

// Returns the median and standard deviation of numbers in `nums`.
function makeDataPoint(nums: number[]): DataPoint {
  const sorted = nums.slice();
  sorted.sort();
  const lenDiv2 = Math.floor(nums.length / 2);
  let median = nums[lenDiv2];
  if (nums.length % 2 === 0) {
    median += nums[lenDiv2 - 1];
    median /= 2;
  }

  const mean = getMean(nums);
  const stddev = Math.sqrt(getMean(nums.map(n => Math.pow(n - mean, 2))));

  return {median, stddev};
}

// Generates a light and dark version of some color randomly (but stable) derived from `str`.
function getSolidAndAlphaColors(str: string): [string, string] {
  // getHashOfValue() returns a Uint8Array, so pull out the first 3 8-bit numbers - which will be in
  // the range [0, 255] - to generate a full RGB colour.
  let [r, g, b] = getHashOfValue(str).digest;
  // Invert if it's too dark.
  if (getMean([r, g, b]) < 128) {
    [r, g, b] = [r, g, b].map(c => c + 128);
  }
  return [`rgb(${r}, ${g}, ${b})`, `rgba(${r}, ${g}, ${b}, 0.2)`];
}

// Returns the keys of `map`.
function keys<K: Value, V: Value>(map: NomsMap<K, V>): Promise<K[]> {
  const keys = [];
  return map.forEach((_, key) => {
    keys.push(key);
  }).then(() => keys);
}

// Returns the mean of `nums`.
function getMean(nums: number[]): number {
  return nums.reduce((t, n) => t + n, 0) / nums.length;
}
