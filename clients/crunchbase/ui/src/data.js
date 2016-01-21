// @flow

import {
  invariant,
  Kind,
  makeCompoundType,
  makePrimitiveType,
  makeType,
  NomsMap,
  NomsSet,
  readValue,
  Ref,
  registerPackage,
  SetLeafSequence,
  Struct,
} from 'noms';
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
  _datasetP: ?Promise<NomsMap<Ref, Ref>>;
  _packageP: ?Promise<Package>;

  _categorySetP: ?Promise<NomsSet<Ref>>;
  _timeSetP: ?Promise<NomsSet<Ref>>;
  _seedSetP: ?Promise<NomsSet<Ref>>;
  _seriesASetP: ?Promise<NomsSet<Ref>>;
  _seriesBSetP: ?Promise<NomsSet<Ref>>;

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

    this._timeSetP = null;
    this._categorySetP = null;
    this._seedSetP = null;
    this._seriesASetP = null;
    this._seriesBSetP = null;

    this._data = null;
    this._time = null;
    this._category = '';
  }

  async _getDataset(): Promise<NomsMap<Ref, Ref>> {
    if (this._datasetP) {
      return this._datasetP;
    }
    return this._datasetP = getDataset(this._datasetId, this._store);
  }

  async _getPackage(): Promise<Package> {
    if (this._packageP) {
      return this._packageP;
    }

    const ds = await this._getDataset();
    this._packageP = getKeyPackage(ds, this._store);
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

    const store = this._store;
    const getAmountRaised = (r: Ref): Promise<number> =>
        readValue(r, store).then(round => round.get('RaisedAmountUsd'));

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

  async _getKeyRef(p: KeyParam): Promise<Ref> {
    const Key = await this._getKeyClass();
    let k;
    if (p.Quarter !== undefined) {
      const Quarter = await this._getQuarterClass();
      k = new Key({Quarter: new Quarter(p)});
    } else {
      k = new Key(p);
    }
    return k.ref;
  }

  async _getSetOfRounds(p: KeyParam): Promise<NomsSet<Ref>> {
    const r = await this._getKeyRef(p);
    invariant(this._datasetP);
    const map = await this._datasetP;
    const set = await map.get(r);
    if (set === undefined) {
      // TODO: Cleanup the NomsSet api (it shouldn't be this hard to create an emptySet)
      return new NomsSet(this._store, setTr, new SetLeafSequence(setTr, []));
    }

    return set;
  }
}

// TODO: This is actually the wrong type. Fix when we have JS codegen.
const setTr = makeCompoundType(Kind.Set, makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Value)));

/**
 * Loads the first key in the index and gets the package from the type.
 */
async function getKeyPackage(index: NomsMap<Ref, Ref>, store: ChunkStore):
    Promise<Package> {
  const kv = await index.first();
  invariant(kv);
  const ref = kv[0];
  const key: Struct = await readValue(ref, store);
  invariant(key);
  const pkg: Package = await readValue(key.type.packageRef, store);
  invariant(pkg);
  registerPackage(pkg);
  return pkg;
}

function getStructClass(pkg, name) {
  const keyIndex = pkg.types.findIndex(t => t.name === name);
  const type = makeType(pkg.ref, keyIndex);
  const typeDef = pkg.types[keyIndex];

  return class extends Struct {
    constructor(data) {
      super(type, typeDef, data);
    }
  };
}

async function getDataset(id: string, httpStore: ChunkStore): Promise<NomsMap<Ref, Ref>> {
  const rootRef = await httpStore.getRoot();
  const datasets: Map<string, Ref> = await readValue(rootRef, httpStore);
  const commitRef = await datasets.get(id);
  invariant(commitRef);
  const commit: Struct = await readValue(commitRef, httpStore);
  invariant(commit);
  return commit.get('value');
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
