// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Layout from './layout.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {
  Blob,
  Database,
  Hash,
  HttpBatchStore,
  IndexedMetaSequence,
  invariant,
  List,
  ListLeafSequence,
  Map,
  MapLeafSequence,
  OrderedMetaSequence,
  Ref,
  Set,
  SetLeafSequence,
  Struct,
  StructMirror,
} from '@attic/noms';
import type {StructFieldMirror} from '@attic/noms';
import {layout, TreeNode} from './buchheim.js';
import type {NodeGraph} from './buchheim.js';

const data: NodeGraph = {nodes: {}, links: {}};
let rootHash: Hash;
let database: Database;

let renderNode: ?HTMLElement;
let params;

window.onload = load;
window.onpopstate = load;
window.onresize = render;

function load() {
  renderNode = document.getElementById('splore');

  params = {};
  const paramsIdx = location.href.indexOf('?');
  if (paramsIdx > -1) {
    decodeURIComponent(location.href.slice(paramsIdx + 1)).split('&').forEach(pair => {
      const [k, v] = pair.split('=');
      params[k] = v;
    });
  }

  if (!params.db) {
    renderPrompt();
    return;
  }

  const opts = {};
  if (params.token) {
    opts['headers'] = {Authorization: `Bearer ${params.token}`};
  }

  const httpStore = new HttpBatchStore(params.db, undefined, opts);
  database = new Database(httpStore);

  const setRootHash = hash => {
    rootHash = hash;
    handleChunkLoad(hash, hash);
  };

  if (params.hash) {
    setRootHash(Hash.parse(params.ref));
  } else {
    httpStore.getRoot().then(setRootHash);
  }
}

function formatKeyString(v: any): string {
  if (v instanceof Ref) {
    v = v.targetHash;
  }
  if (v instanceof Hash) {
    return v.toString().substring(5, 11);
  }

  return String(v);
}

function handleChunkLoad(hash: Hash, val: any, fromHash: ?string) {
  let counter = 0;

  function processMetaSequence(id, sequence: IndexedMetaSequence | OrderedMetaSequence,
                               name: string) {
    data.nodes[id] = {name: name};
    sequence.items.forEach(tuple => {
      const kid = process(hash, formatKeyString(tuple.value), id);
      if (kid) {
        data.nodes[kid].isOpen = true;

        process(hash, tuple.ref, kid);
      } else {
        throw new Error('No kid id.');
      }
    });
  }

  function process(hash, val, fromId): ?string {
    const t = typeof val;
    if (t === 'undefined') {
      return null;
    }

    // Assign a unique ID to this node.
    // We don't use the noms hash because we only want to represent values as shared in the graph if
    // they are actually in the same chunk.
    let id;
    if (val instanceof Ref) {
      val = val.targetHash;
    }

    if (val instanceof Hash) {
      id = val.toString();
    } else {
      id = hash.toString() + '/' + counter++;
    }

    // Populate links.
    if (fromId) {
      (data.links[fromId] || (data.links[fromId] = [])).push(id);
    }

    if (t === 'boolean' || t === 'number' || t === 'string') {
      data.nodes[id] = {name: String(val)};
    } else if (val instanceof Blob) {
      data.nodes[id] = {name: `Blob (${val.length})`};
    } else if (val instanceof List) {
      const sequence = val.sequence;
      if (sequence instanceof ListLeafSequence) {
        data.nodes[id] = {name: `List (${val.length})`};
        sequence.items.forEach(c => process(hash, c, id));
      } else {
        invariant(sequence instanceof IndexedMetaSequence);
        processMetaSequence(id, sequence, 'ListNode');
      }
    } else if (val instanceof Set) {
      const sequence = val.sequence;
      if (sequence instanceof SetLeafSequence) {
        data.nodes[id] = {name: `Set (${val.size})`};
        sequence.items.forEach(c => process(hash, c, id));
      } else {
        invariant(sequence instanceof OrderedMetaSequence);
        processMetaSequence(id, sequence, 'SetNode');
      }
    } else if (val instanceof Map) {
      const sequence = val.sequence;
      if (sequence instanceof MapLeafSequence) {
        data.nodes[id] = {name: `Map (${val.size})`};
        sequence.items.forEach(entry => {
          const [k, v] = entry;
          // TODO: handle non-string keys
          const kid = process(hash, k, id);
          if (kid) {
            data.nodes[kid].isOpen = true;

            process(hash, v, kid);
          } else {
            throw new Error('No kid id.');
          }
        });
      } else {
        invariant(sequence instanceof OrderedMetaSequence);
        processMetaSequence(id, sequence, 'MapNode');
      }
    } else if (val instanceof Hash) {
      const refStr = val.toString();
      data.nodes[id] = {
        canOpen: true,
        name: refStr.substr(5, 6),
        hash: val,
      };
    } else if (val instanceof Struct) {
      // Struct
      // Make a variable to the struct to work around Flow bug.
      const mirror = new StructMirror(val);
      const structName = mirror.name;
      data.nodes[id] = {name: structName};

      mirror.forEachField((f: StructFieldMirror) => {
        const v = f.value;
        const kid = process(hash, f.name, id);
        if (kid) {
          // Start struct keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(hash, v, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
    } else {
      invariant(val !== null && val !== undefined);
      console.log('Unsupported type', val.constructor.name, val); // eslint-disable-line no-console
    }

    return id;
  }

  process(hash, val, fromHash);
  render();
}

function handleNodeClick(e: MouseEvent, id: string) {
  if (e.button === 0 && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey) {
    e.preventDefault();
  }

  if (id.indexOf('/') > -1) {
    if (data.links[id] && data.links[id].length > 0) {
      data.nodes[id].isOpen = !data.nodes[id].isOpen;
      render();
    }
  } else {
    data.nodes[id].isOpen = !data.nodes[id].isOpen;
    if (data.links[id] || !data.nodes[id].isOpen) {
      render();
    } else {
      const hash = Hash.parse(id);
      database.readValue(hash).then(value => {
        handleChunkLoad(hash, value, id);
      });
    }
  }
}

class Prompt extends React.Component<void, {}, void> {
  render(): React.Element {
    const fontStyle: {[key: string]: any} = {
      fontFamily: 'Menlo',
      fontSize: 14,
    };
    const inputStyle = Object.assign(fontStyle, {}, {width: '50ex', marginBottom: '0.5em'});
    return <div style={{display: 'flex', height: '100%', alignItems: 'center',
      justifyContent: 'center'}}>
      <div style={fontStyle}>
        Can haz database?
        <form style={{margin:'0.5em 0'}} onSubmit={e => this._handleOnSubmit(e)}>
          <input type='text' ref='db' autoFocus={true} style={inputStyle}
            defaultValue={params.db || 'http://api.noms.io/-/ds/[user]'}
            placeholder='noms database URL'
          />
          <input type='text' ref='token' style={inputStyle}
            defaultValue={params.token}
            placeholder='auth token'
          />
          <input type='text' ref='ref' style={inputStyle}
            defaultValue={params.ref}
            placeholder='sha1-xyz (ref to jump to)'
          />
          <button type='submit'>OK</button>
        </form>
      </div>
    </div>;
  }

  _handleOnSubmit(e) {
    e.preventDefault();
    const {db, token, ref} = this.refs;
    let qs = '?db=' + db.value;
    if (token.value) {
      qs += '&token=' + token.value;
    }
    if (ref.value) {
      qs += '&ref=' + ref.value;
    }
    window.history.pushState({}, undefined, qs);
    load();
  }
}

function renderPrompt() {
  ReactDOM.render(<Prompt/>, renderNode);
}

function render() {
  const dt = new TreeNode(data, rootHash.toString(), null, 0, 0, {});
  layout(dt);
  ReactDOM.render(
    <Layout tree={dt} data={data} onNodeClick={handleNodeClick} nomsStore={params.db}/>,
    renderNode);
}
