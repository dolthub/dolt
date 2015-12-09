// @flow

import {readValue, Struct, makeType, Ref, registerPackage} from 'noms';
import {invariant} from './assert.js';
import type {ChunkStore, Package} from 'noms';

type RoundTypeEnum = 0 | 1 | 2;
const Seed = 0;
const SeriesA = 1;
const SeriesB = 2;

type QuarterEnum = 0 | 1 | 2 | 3;

type KeyParam = {
  Year: number,
  Quarter?: QuarterEnum
} | {
  Category: string
} | {
  RoundType: RoundTypeEnum
};

type TimeOption = {
  Year: number,
  Quarter?: QuarterEnum
};

type DataPoint = {x: number, y: number};
type DataEntry = {values: Array<DataPoint>, key: string, color?: string};
export type DataArray = Array<DataEntry>;

export default class DataManager {
  _store: ChunkStore;
  _datasetId: string;
  _keyClass: any;
  _quarterClass: any;
  _index: Map<string, Ref>;
  _datasetP: ?Promise<Map<Ref, Ref>>;
  _packageP: ?Promise<Package>;

  _categorySetP: ?Promise<Set<Struct>>;
  _timeSetP: ?Promise<Set<Struct>>;
  _seedSetP: ?Promise<Set<Struct>>;
  _seriesASetP: ?Promise<Set<Struct>>;
  _seriesBSetP: ?Promise<Set<Struct>>;

  _data: ?DataArray;
  _time: ?TimeOption;
  _category: string;

  constructor(store: ChunkStore, datasetId: string) {
    this._datasetId = datasetId;
    this._store = store;
    this._keyClass = null;
    this._quarterClass = null;
    this._datasetP = null;
    this._packageP = null;
    this._index = new Map();

    this._timeSetP = null;
    this._categorySetP = null;
    this._seedSetP = null;
    this._seriesASetP = null;
    this._seriesBSetP = null;

    this._data = null;
    this._time = null;
    this._category = '';
  }

  async _getDataset(): Promise<Map<Ref, Ref>> {
    if (this._datasetP) {
      return this._datasetP;
    }
    return this._datasetP = getDataset(this._datasetId, this._store);
  }

  async _getPackage(): Promise<Package> {
    if (this._packageP) {
      return this._packageP;
    }

    let ds = await this._getDataset();
    this._packageP = getKeyPackage(ds, this._store);
    this._index = convertMap(ds);
    invariant(this._packageP);
    return this._packageP;
  }

  async _getKeyClass(): Promise<any> {
    if (this._keyClass) return this._keyClass;
    let pkg = await this._getPackage();
    return this._keyClass = getStructClass(pkg, 'Key');
  }

  async _getQuarterClass(): Promise<any> {
    if (this._quarterClass) return this._quarterClass;
    let pkg = await this._getPackage();
    return this._quarterClass = getStructClass(pkg, 'Quarter');
  }

  _setTime(time: TimeOption) {
    let t = this._time;
    if (!t || t.Year !== time.Year || t.Quarter !== time.Quarter) {
      this._time = time;
      this._timeSetP = this._getSetOfRounds(time);
      this._data = null;
    }
  }

  _setCategory(category: string) {
    if (this._category !== category) {
      this._category = category;
      this._categorySetP = this._getSetOfRounds({Category: category});
      this._data = null;
    }
  }

  _createRounds() {
    this._seedSetP = this._getSetOfRounds({RoundType: Seed});
    this._seriesASetP = this._getSetOfRounds({RoundType: SeriesA});
    this._seriesBSetP = this._getSetOfRounds({RoundType: SeriesB});
  }

  async getData(time: TimeOption, category: string): any {
    if (!this._seedSetP) {
      this._createRounds();
    }
    this._setTime(time);
    this._setCategory(category);

    if (this._data) return this._data;

    invariant(this._seedSetP && this._seriesASetP && this._seriesBSetP &&
              this._timeSetP && this._categorySetP);
    let [seedSet, seriesASet, seriesBSet, timeSet, categorySet] =
        await Promise.all([this._seedSetP, this._seriesASetP, this._seriesBSetP,
            this._timeSetP, this._categorySetP]);

    let baseSet = intersectRounds(timeSet, categorySet);

    return this._data = [
      {
        values: percentiles(intersectRounds(baseSet, seedSet)),
        key: 'Seed'
      },
      {
        values: percentiles(intersectRounds(baseSet, seriesASet)),
        key: 'A'
      },
      {
        values: percentiles(intersectRounds(baseSet, seriesBSet)),
        key: 'B'
      }
    ];
  }

  async _getKeyRef(p: KeyParam): Promise<Ref> {
    const Key = await this._getKeyClass();
    let k;
    if (p.Quarter !== undefined) {
      let Quarter = await this._getQuarterClass();
      k = new Key({Quarter: new Quarter(p)});
    } else {
      k = new Key(p);
    }
    return k.ref;
  }

  async _getSetOfRounds(p: KeyParam): Promise<Set<Struct>> {
    let s = (await this._getKeyRef(p)).toString();
    let v = this._index.get(s);
    if (v === undefined) {
      return new Set();
    }
    let set = readValue(v, this._store);
    return set || new Set();
  }

}

/**
 * Loads the first key in the index and gets the package from the type.
 */
async function getKeyPackage(index: Map<Ref, Ref>, store: ChunkStore):
    Promise<Package> {
  let ref;
  for (let v of index.keys()) {
    ref = v;
    break;
  }
  invariant(ref instanceof Ref);
  let key = await readValue(ref, store);
  let pkg = await readValue(key.type.packageRef, store);
  registerPackage(pkg);
  return pkg;
}

function getStructClass(pkg, name) {
  let keyIndex = pkg.types.findIndex(t => t.name === name);
  let type = makeType(pkg.ref, keyIndex);
  let typeDef = pkg.types[keyIndex];

  return class extends Struct {
    constructor(data) {
      super(type, typeDef, data);
    }
  };
}

async function getDataset(id: string, httpStore: ChunkStore): any {
  let rootRef = await httpStore.getRoot();
  let datasets = await readValue(rootRef, httpStore);
  let commitRef = datasets.get(id);
  let commit = await readValue(commitRef, httpStore);
  return commit.get('value');
}

function convertMap<T>(map: Map<Ref, T>): Map<string, T> {
  let m = new Map();
  map.forEach((v, k) => {
    m.set(k.toString(), v);
  });
  return m;
}

function intersectRounds(a: Set<Struct>, b: Set<Struct>): Set<Struct> {
  let sa = new Set();
  a.forEach(v => {
    sa.add(v.ref.toString());
  });
  let s = new Set();
  b.forEach(v => {
    if (sa.has(v.ref.toString())) {
      s.add(v);
    }
  });
  return s;
}

function percentiles(s: Set<Struct>): Array<{x: number, y: number}> {
  let arr: Array<number> = [];
  for (let round of s) {
    let v = round.get('Raised');

    if (v > 0) {
      arr.push(v);
    }

  }
  arr.sort((a, b) => a - b);
  let len = arr.length;
  if (len === 0) {
    return [];
  }
  if (len === 1) {
    return [{x: 0, y: arr[0]}, {x: 1, y: arr[0]}];
  }
  return arr.map((y, i) => {
    let x = i / (len - 1);
    return {x, y};
  });
}
