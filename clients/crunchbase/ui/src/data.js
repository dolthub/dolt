// @flow

import {
  createStructClass,
  Dataset,
  DataStore,
  invariant,
  makeRefType,
  makeSetType,
  makeType,
  newSet,
  NomsMap,
  NomsSet,
  notNull,
  RefValue,
  registerPackage,
  Struct,
  valueType,
} from '@attic/noms';
import type {Commit, Package} from '@attic/noms';

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
  _dataset: Dataset;
  _keyClass: ?Class<Struct>;
  _quarterClass: ?Class<Struct>;
  _indexP: Promise<NomsMap>;
  _packageP: ?Promise<Package>;

  _categorySetP: ?Promise<NomsSet<RefValue>>;
  _timeSetP: ?Promise<NomsSet<RefValue>>;
  _seedSetP: ?Promise<NomsSet<RefValue>>;
  _seriesASetP: ?Promise<NomsSet<RefValue>>;
  _seriesBSetP: ?Promise<NomsSet<RefValue>>;

  _data: ?DataArray;
  _time: ?TimeOption;
  _category: string;

  constructor(dataset: Dataset) {
    this._dataset = dataset;
    this._indexP = this._dataset.head().then((commit: ?Commit) => {
      const v = notNull(commit).value;
      invariant(v instanceof NomsMap);
      return v;
    });

    this._keyClass = null;
    this._quarterClass = null;
    this._packageP = null;

    this._timeSetP = null;
    this._categorySetP = null;
    this._seedSetP = null;
    this._seriesASetP = null;
    this._seriesBSetP = null;

    this._data = null;
    this._time = null;
    this._category = '';
  }

  async _getPackage(): Promise<Package> {
    if (this._packageP) {
      return this._packageP;
    }

    const index = await this._indexP;
    this._packageP = getKeyPackage(index, this._dataset.store);
    return this._packageP;
  }

  async _getKeyClass(): Promise<any> {
    if (this._keyClass) return this._keyClass;
    const pkg = await this._getPackage();
    return this._keyClass = getStructClass(pkg, 'Key');
  }

  async _getQuarterClass(): Promise<any> {
    if (this._quarterClass) return this._quarterClass;
    const pkg = await this._getPackage();
    return this._quarterClass = getStructClass(pkg, 'Quarter');
  }

  _setTime(time: TimeOption) {
    const t = this._time;
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
    const [seedSet, seriesASet, seriesBSet, timeSet, categorySet] =
        await Promise.all([this._seedSetP, this._seriesASetP, this._seriesBSetP,
            this._timeSetP, this._categorySetP]);

    const store = this._dataset.store;
    const getAmountRaised = (r: RefValue): Promise<number> =>
        store.readValue(r.targetRef).then(round => round.get('RaisedAmountUsd'));

    const [seedData, seriesAData, seriesBData] = await Promise.all([
      seedSet.intersect(categorySet, timeSet).then(set => set.map(getAmountRaised)),
      seriesASet.intersect(categorySet, timeSet).then(set => set.map(getAmountRaised)),
      seriesBSet.intersect(categorySet, timeSet).then(set => set.map(getAmountRaised)),
    ]);

    return this._data = [
      {
        values: percentiles(seedData),
        key: 'Seed',
      },
      {
        values: percentiles(seriesAData),
        key: 'A',
      },
      {
        values: percentiles(seriesBData),
        key: 'B',
      },
    ];
  }

  async _getKeyRef(p: KeyParam): Promise<RefValue> {
    const Key = await this._getKeyClass();
    let k;
    if (p.Quarter !== undefined) {
      const Quarter = await this._getQuarterClass();
      k = new Key({Quarter: new Quarter(p)});
    } else {
      k = new Key(p);
    }
    return new RefValue(k.ref, makeRefType(k.type));
  }

  async _getSetOfRounds(p: KeyParam): Promise<NomsSet<RefValue>> {
    const [r, index] = await Promise.all([this._getKeyRef(p), this._indexP]);
    const set = await index.get(r);
    if (set === undefined) {
      return newSet([], setType);
    }

    invariant(set);
    // Update the type to something that is correct.
    // An alternative would be to hardcode the ref/ordinal.
    setType = set.type;
    return set;
  }
}

// TODO: This is actually the wrong type. Fix when we have JS codegen.
let setType = makeSetType(makeRefType(valueType));

/**
 * Loads the first key in the index and gets the package from the type.
 */
async function getKeyPackage(index: NomsMap<RefValue, NomsSet<RefValue>>, store: DataStore):
    Promise<Package> {
  const kv = await index.first();
  invariant(kv);
  const ref = kv[0];
  const key: Struct = await store.readValue(ref.targetRef);
  invariant(key);
  const pkg: Package = await store.readValue(key.type.packageRef);
  invariant(pkg);
  registerPackage(pkg);
  return pkg;
}

function getStructClass(pkg, name) {
  const keyIndex = pkg.types.findIndex(t => t.name === name);
  const type = makeType(pkg.ref, keyIndex);
  const typeDef = pkg.types[keyIndex];
  return createStructClass(type, typeDef);
}

function percentiles(s: Array<number>): Array<{x: number, y: number}> {
  const arr: Array<number> = [];
  s.forEach(v => {
    if (v > 0) {
      arr.push(v);
    }
  });

  arr.sort((a, b) => a - b);
  const len = arr.length;
  if (len === 0) {
    return [];
  }
  if (len === 1) {
    return [{x: 0, y: arr[0]}, {x: 1, y: arr[0]}];
  }
  return arr.map((y, i) => {
    const x = i / (len - 1);
    return {x, y};
  });
}
