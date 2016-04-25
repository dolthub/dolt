// @flow

import Ref from './ref.js';
import MemoryStore from './memory-store.js';
import BatchStore from './batch-store.js';
import type {ChunkStore} from './chunk-store.js';
import type {UnsentReadMap, WriteRequest} from './batch-store.js';

export function makeTestingBatchStore(): BatchStore {
  return new BatchStore(3, new BatchStoreAdaptorDelegate(new MemoryStore()));
}

export default class BatchStoreAdaptor extends BatchStore {
  constructor(cs: ChunkStore, maxReads: number = 3) {
    super(maxReads, new BatchStoreAdaptorDelegate(cs));
  }
}

export class BatchStoreAdaptorDelegate {
  _cs: ChunkStore;

  constructor(cs: ChunkStore) {
    this._cs = cs;
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    Object.keys(reqs).forEach(refStr => {
      this._cs.get(Ref.parse(refStr)).then(chunk => { reqs[refStr](chunk); });
    });
  }

  async writeBatch(reqs: Array<WriteRequest>): Promise<void> {
    reqs.forEach(req => {
      this._cs.put(req.c);
    });
  }

  async getRoot(): Promise<Ref> {
    return this._cs.getRoot();
  }

  async updateRoot(current: Ref, last: Ref): Promise<boolean> {
    return this._cs.updateRoot(current, last);
  }
}
