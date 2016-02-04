// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct from './struct.js';
import type {ChunkStore} from './chunk_store.js';
import {Field, makeCompoundType, makePrimitiveType, makeStructType, makeType,
  Type} from './type.js';
import {Kind} from './noms_kind.js';
import {newMap, NomsMap} from './map.js';
import {Package, registerPackage} from './package.js';
import {readValue} from './read_value.js';

type DatasTypes = {
  commitTypeDef: Type,
  datasPackage: Package,
  commitType: Type,
  commitSetType: Type,
  commitMapType: Type,
};

let emptyCommitMap: Promise<NomsMap<string, Ref>>;
function getEmptyCommitMap(): Promise<NomsMap<string, Ref>> {
  if (!emptyCommitMap) {
    emptyCommitMap = newMap(getDatasTypes().commitMapType, []);
  }
  return emptyCommitMap;
}


let datasTypes: DatasTypes;
export function getDatasTypes(): DatasTypes {
  if (!datasTypes) {
    const commitTypeDef = makeStructType('Commit', [
      new Field('value', makePrimitiveType(Kind.Value), false),
      new Field('parents', makeCompoundType(Kind.Set,
        makeCompoundType(Kind.Ref, makeType(new Ref(), 0))), false),
    ], []);

    const datasPackage = new Package([commitTypeDef], []);
    registerPackage(datasPackage);

    const commitType = makeType(datasPackage.ref, 0);

    const commitSetType = makeCompoundType(Kind.Set,
      makeCompoundType(Kind.Ref, makeType(datasPackage.ref, 0)));

    const commitMapType =
      makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                 makeCompoundType(Kind.Ref, makeType(datasPackage.ref, 0)));

    datasTypes = {
      commitTypeDef,
      datasPackage,
      commitType,
      commitSetType,
      commitMapType,
    };
  }

  return datasTypes;
}

export class DataStore {
  _cs: ChunkStore;
  _datasets: Promise<NomsMap<string, Ref>>;

  constructor(cs: ChunkStore) {
    this._cs = cs;
    this._datasets = this._datasetsFromRootRef();
  }

  getRoot(): Promise<Ref> {
    return this._cs.getRoot();
  }

  updateRoot(current: Ref, last: Ref): Promise<boolean> {
    return this._cs.updateRoot(current, last);
  }

  get(ref: Ref): Promise<Chunk> {
    return this._cs.get(ref);
  }

  has(ref: Ref): Promise<boolean> {
    return this._cs.has(ref);
  }

  put(c: Chunk): void {
    this._cs.put(c);
  }

  close() {}

  _datasetsFromRootRef(): Promise<NomsMap<string, Ref>> {
    return this._cs.getRoot().then(rootRef => {
      if (rootRef.isEmpty()) {
        return getEmptyCommitMap();
      }

      return readValue(rootRef, this._cs);
    });
  }

  head(datasetID: string): Promise<?Struct> {
    return this._datasets.then(
      datasets => datasets.get(datasetID).then(commitRef => commitRef ?
                                                            readValue(commitRef, this._cs) : null));
  }

  datasets(): Promise<NomsMap<string, Ref>> {
    return this._datasets;
  }
}
