// @flow

import Ref from './ref.js';
import {default as RefValue} from './ref-value.js';
import {newStruct} from './struct.js';
import type {NomsMap} from './map.js';
import type {NomsSet} from './set.js';
import type {valueOrPrimitive} from './value.js';
import ValueStore from './value-store.js';
import BatchStore from './batch-store.js';
import {
  Field,
  makeRefType,
  makeStructType,
  makeSetType,
  makeMapType,
  Type,
  stringType,
  valueType,
} from './type.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import type {Commit} from './commit.js';

type DatasTypes = {
  commitType: Type,
  commitSetType: Type,
  refOfCommitType: Type,
  commitMapType: Type,
};

let emptyCommitMap: Promise<NomsMap<string, RefValue<Commit>>>;
function getEmptyCommitMap(): Promise<NomsMap<string, RefValue<Commit>>> {
  if (!emptyCommitMap) {
    emptyCommitMap = newMap([], getDatasTypes().commitMapType);
  }
  return emptyCommitMap;
}

let datasTypes: DatasTypes;
export function getDatasTypes(): DatasTypes {
  if (!datasTypes) {
    // struct Commit {
    //   value: Value
    //   parents: Set<Ref<Commit>>
    // }
    const commitType = makeStructType('Commit', [
      new Field('value', valueType),
    ]);
    const refOfCommitType = makeRefType(commitType);
    const commitSetType = makeSetType(refOfCommitType);
    commitType.desc.fields.push(new Field('parents', commitSetType));
    const commitMapType = makeMapType(stringType, refOfCommitType);
    datasTypes = {
      commitType,
      refOfCommitType,
      commitSetType,
      commitMapType,
    };
  }

  return datasTypes;
}

export default class DataStore extends ValueStore {
  _bs: BatchStore;
  _cacheSize: number;
  _datasets: Promise<NomsMap<string, RefValue<Commit>>>;

  constructor(bs: BatchStore, cacheSize: number = 0) {
    super(bs, cacheSize);
    // bs and cacheSize should only be used when creating a new DataStore instance in commit()
    this._bs = bs;
    this._cacheSize = cacheSize;
    this._datasets = this._datasetsFromRootRef(bs.getRoot());
  }

  _datasetsFromRootRef(rootRef: Promise<Ref>): Promise<NomsMap<string, RefValue<Commit>>> {
    return rootRef.then(rootRef => {
      if (rootRef.isEmpty()) {
        return getEmptyCommitMap();
      }

      return this.readValue(rootRef);
    });
  }

  headRef(datasetID: string): Promise<?RefValue<Commit>> {
    return this._datasets.then(datasets => datasets.get(datasetID));
  }

  head(datasetID: string): Promise<?Commit> {
    return this.headRef(datasetID).then(hr => hr ? this.readValue(hr.targetRef) : null);
  }

  datasets(): Promise<NomsMap<string, RefValue<Commit>>> {
    return this._datasets;
  }

  async _descendsFrom(commit: Commit, currentHeadRef: RefValue<Commit>): Promise<boolean> {
    let ancestors = commit.parents;
    while (!(await ancestors.has(currentHeadRef))) {
      if (ancestors.isEmpty()) {
        return false;
      }
      ancestors = await getAncestors(ancestors, this);
    }
    return true;
  }

  async commit(datasetId: string, commit: Commit): Promise<DataStore> {
    const currentRootRefP = this._bs.getRoot();
    const datasetsP = this._datasetsFromRootRef(currentRootRefP);
    let currentDatasets = await (datasetsP:Promise<NomsMap>);
    const currentRootRef = await currentRootRefP;
    const commitRef = this.writeValue(commit);

    if (!currentRootRef.isEmpty()) {
      const currentHeadRef = await currentDatasets.get(datasetId);
      if (currentHeadRef) {
        if (commitRef.equals(currentHeadRef)) {
          return this;
        }
        if (!await this._descendsFrom(commit, currentHeadRef)) {
          throw new Error('Merge needed');
        }
      }
    }

    currentDatasets = await currentDatasets.set(datasetId, commitRef);
    const newRootRef = this.writeValue(currentDatasets).targetRef;
    if (await this._bs.updateRoot(newRootRef, currentRootRef)) {
      return new DataStore(this._bs, this._cacheSize);
    }

    throw new Error('Optimistic lock failed');
  }
}

async function getAncestors(commits: NomsSet<RefValue<Commit>>, store: DataStore):
    Promise<NomsSet<RefValue<Commit>>> {
  let ancestors = await newSet([], getDatasTypes().commitSetType);
  await commits.map(async (commitRef) => {
    const commit = await store.readValue(commitRef.targetRef);
    await commit.parents.map(async (ref) => ancestors = await ancestors.insert(ref));
  });
  return ancestors;
}

export function newCommit(value: valueOrPrimitive,
                          parentsArr: Array<RefValue<Commit>> = []): Promise<Commit> {
  const types = getDatasTypes();
  return newSet(parentsArr, types.commitSetType).then(parents =>
      newStruct(types.commitType, {value, parents}));
}