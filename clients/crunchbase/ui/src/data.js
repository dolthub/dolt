// @flow

import {readValue, Struct, makeType, Ref, registerPackage} from 'noms';
import type {ChunkStore, Package} from 'noms';
import {invariant, Kind, NomsMap, NomsSet, SetLeafSequence, makeCompoundType, makePrimitiveType} from 'noms';

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

  _categorySetP: ?Promise<NomsSet<Struct>>;
  _timeSetP: ?Promise<NomsSet<Struct>>;
  _seedSetP: ?Promise<NomsSet<Struct>>;
  _seriesASetP: ?Promise<NomsSet<Struct>>;
  _seriesBSetP: ?Promise<NomsSet<Struct>>;

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

    let ds = await this._getDataset();
    this._packageP = getKeyPackage(ds, this._store);
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

    let baseSet = await timeSet.intersect(categorySet);
    let sets = await Promise.all([baseSet.intersect(seedSet),
          baseSet.intersect(seriesASet), baseSet.intersect(seriesBSet)]);
    let ptiles = await Promise.all([percentiles(sets[0]), percentiles(sets[1]),
          percentiles(sets[2])]);

    return this._data = [
      {
        values: ptiles[0],
        key: 'Seed'
      },
      {
        values: ptiles[1],
        key: 'A'
      },
      {
        values: ptiles[2],
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

  async _getSetOfRounds(p: KeyParam): Promise<NomsSet<Struct>> {
    let r = await this._getKeyRef(p);
    invariant(this._datasetP);
    let map = await this._datasetP;
    let setRef = await map.get(r);
    if (setRef === undefined) {
      // TODO: Cleanup the NomsSet api (it shouldn't be this hard to create an emptySet)
      return new NomsSet(this._store, setTr, new SetLeafSequence(setTr, []));
    }

    return readValue(setRef, this._store);
  }
}

// TODO: This is actually the wrong type. Fix when we have JS codegen.
const setTr = makeCompoundType(Kind.Set, makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Value)));

/**
 * Loads the first key in the index and gets the package from the type.
 */
async function getKeyPackage(index: NomsMap<Ref, Ref>, store: ChunkStore):
    Promise<Package> {
  let kv = await index.first();
  invariant(kv);
  let ref = kv[0];
  let key: Struct = await readValue(ref, store);
  invariant(key);
  let pkg: Package = await readValue(key.type.packageRef, store);
  invariant(pkg);
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

async function getDataset(id: string, httpStore: ChunkStore): Promise<NomsMap<Ref, Ref>> {
  let rootRef = await httpStore.getRoot();
  let datasets: Map<string, Ref> = await readValue(rootRef, httpStore);
  let commitRef = await datasets.get(id);
  invariant(commitRef);
  let commit: Struct = await readValue(commitRef, httpStore);
  invariant(commit);
  return commit.get('value');
}


async function percentiles(s: NomsSet<Struct>): Promise<Array<{x: number, y: number}>> {
  let arr: Array<number> = [];
  await s.forEach(round => {
    let v = round.get('Raised');

    if (v > 0) {
      arr.push(v);
    }
  });

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
