// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Layout from './layout.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {
  Blob,
  Collection,
  Database,
  Hash,
  HttpBatchStore,
  IndexedMetaSequence,
  invariant,
  kindToString,
  List,
  Map,
  OrderedMetaSequence,
  Ref,
  Set,
  Struct,
  StructMirror,
} from '@attic/noms';
import type {StructFieldMirror, Value} from '@attic/noms';
import {layout, TreeNode} from './buchheim.js';
import type {NodeGraph} from './buchheim.js';
import {filesize} from 'humanize';

const data: NodeGraph = {nodes: {}, links: {}};
let rootHash: Hash;
let database: Database;

let renderNode: ?HTMLElement;
let params = {};

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

  const setRootHash = (hash: Hash) => {
    rootHash = hash;
    handleChunkLoad(hash, hash);
  };

  if (params.hash) {
    const hash = Hash.parse(params.hash);
    invariant(hash);
    setRootHash(hash);
  } else {
    httpStore.getRoot().then(setRootHash);
  }
}

function formatKeyString(v: any): string {
  if (v instanceof Ref) {
    v = v.targetHash;
  }
  if (v instanceof Hash) {
    return v.toString().substring(0, 10);
  }

  return String(v);
}

function handleChunkLoad(hash: Hash, val: any, fromHash: ?string) {
  let counter = 0;

  function processMetaSequence(id, sequence: IndexedMetaSequence | OrderedMetaSequence<any>,
                               name: string) {
    data.nodes[id] = {name: name};
    sequence.items.forEach(tuple => {
      const kid = process(hash, formatKeyString(tuple.ref), id);
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
    } else if (val instanceof Collection) {
      const {sequence} = val;
      const ks = kindToString(val.type.kind);
      const size = getSize(val);
      if (sequence instanceof IndexedMetaSequence || sequence instanceof OrderedMetaSequence) {
        const name = `${ks}Node (${size})`;
        processMetaSequence(id, sequence, name);
      } else {
        const name = `${ks} (${size})`;
        data.nodes[id] = {name};
        if (val instanceof Map) {
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
          sequence.items.forEach(c => process(hash, c, id));
        }
      }
    } else if (val instanceof Hash) {
      const refStr = val.toString();
      data.nodes[id] = {
        canOpen: true,
        name: refStr.substr(0, 10),
        hash: val,
      };
    } else if (val instanceof Struct) {
      // Struct
      // Make a variable to the struct to work around Flow bug.
      const mirror = new StructMirror(val);
      const structName = mirror.name || 'Struct';
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
      invariant(hash);
      database.readValue(hash).then(value => {
        handleChunkLoad(hash, value, id);
      });
    }
  }
}

class Prompt extends React.Component<void, {}, void> {
  render(): React.Element<any> {
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
            defaultValue={params.db || 'https://demo.noms.io/cli-tour'}
            placeholder='noms database URL'
          />
          <input type='text' ref='token' style={inputStyle}
            defaultValue={params.token}
            placeholder='auth token'
          />
          <input type='text' ref='hash' style={inputStyle}
            defaultValue={params.hash}
            placeholder='0123456789abcdefghijklmnopqrstuv (hash to jump to)'
          />
          <button type='submit'>OK</button>
        </form>
      </div>
    </div>;
  }

  _handleOnSubmit(e) {
    e.preventDefault();
    const {db, token, hash} = this.refs;
    let qs = '?db=' + db.value;
    if (token.value) {
      qs += '&token=' + token.value;
    }
    if (hash.value) {
      qs += '&hash=' + hash.value;
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
    <Layout tree={dt} data={data} onNodeClick={handleNodeClick} db={params.db}/>,
    renderNode);
}

function getSize(val: Value): string | number {
  // This was extracted into a function to work around a bug in Flow.
  if (val instanceof List) {
    return val.length;
  }
  if (val instanceof Map || val instanceof Set) {
    return val.size;
  }
  if (val instanceof Blob) {
    return filesize(val.length);
  }
  throw new Error('unreachable');
}
