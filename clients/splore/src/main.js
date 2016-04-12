// @flow

import Layout from './layout.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {
  DataStore,
  HttpStore,
  IndexedMetaSequence,
  invariant,
  ListLeafSequence,
  MapLeafSequence,
  NomsBlob,
  NomsList,
  NomsMap,
  NomsSet,
  OrderedMetaSequence,
  Ref,
  RefValue,
  SetLeafSequence,
  Struct,
} from '@attic/noms';
import {layout, TreeNode} from './buchheim.js';
import type {NodeGraph} from './buchheim.js';

const data: NodeGraph = {nodes: {}, links: {}};
let rootRef: Ref;
let dataStore: DataStore;

let renderNode: ?HTMLElement;

const params = {};

window.onload = load;
window.onpopstate = load;
window.onresize = render;

function load() {
  renderNode = document.getElementById('splore');

  const paramsIdx = location.href.indexOf('?');
  if (paramsIdx > -1) {
    decodeURIComponent(location.href.slice(paramsIdx + 1)).split('&').forEach(pair => {
      const [k, v] = pair.split('=');
      params[k] = v;
    });
  }

  if (!params.store) {
    renderPrompt();
    return;
  }

  const opts = {};
  if (params.token) {
    opts['headers'] = {Authorization: `Bearer ${params.token}`};
  }

  const httpStore = new HttpStore(params.store, undefined, undefined, opts);
  dataStore = new DataStore(httpStore);

  const setRootRef = ref => {
    rootRef = ref;
    handleChunkLoad(ref, ref);
  };

  if (params.ref) {
    setRootRef(Ref.parse(params.ref));
  } else {
    httpStore.getRoot().then(setRootRef);
  }
}

function formatKeyString(v: any): string {
  if (v instanceof RefValue) {
    v = v.targetRef;
  }
  if (v instanceof Ref) {
    return v.toString().substring(5, 11);
  }

  return String(v);
}

function handleChunkLoad(ref: Ref, val: any, fromRef: ?string) {
  let counter = 0;

  function processMetaSequence(id, sequence: IndexedMetaSequence | OrderedMetaSequence,
                               name: string) {
    data.nodes[id] = {name: name};
    sequence.items.forEach(tuple => {
      const kid = process(ref, formatKeyString(tuple.value), id);
      if (kid) {
        data.nodes[kid].isOpen = true;

        process(ref, tuple.ref, kid);
      } else {
        throw new Error('No kid id.');
      }
    });
  }

  function process(ref, val, fromId): ?string {
    const t = typeof val;
    if (t === 'undefined') {
      return null;
    }

    // Assign a unique ID to this node.
    // We don't use the noms ref because we only want to represent values as shared in the graph if
    // they are actually in the same chunk.
    let id;
    if (val instanceof RefValue) {
      val = val.targetRef;
    }

    if (val instanceof Ref) {
      id = val.toString();
    } else {
      id = ref.toString() + '/' + counter++;
    }

    // Populate links.
    if (fromId) {
      (data.links[fromId] || (data.links[fromId] = [])).push(id);
    }

    if (t === 'boolean' || t === 'number' || t === 'string') {
      data.nodes[id] = {name: String(val)};
    } else if (val instanceof NomsBlob) {
      data.nodes[id] = {name: `Blob (${val.length})`};
    } else if (val instanceof NomsList) {
      const sequence = val.sequence;
      if (sequence instanceof ListLeafSequence) {
        data.nodes[id] = {name: `List (${val.length})`};
        sequence.items.forEach(c => process(ref, c, id));
      } else {
        invariant(sequence instanceof IndexedMetaSequence);
        processMetaSequence(id, sequence, 'ListNode');
      }
    } else if (val instanceof NomsSet) {
      const sequence = val.sequence;
      if (sequence instanceof SetLeafSequence) {
        data.nodes[id] = {name: `Set (${val.size})`};
        sequence.items.forEach(c => process(ref, c, id));
      } else {
        invariant(sequence instanceof OrderedMetaSequence);
        processMetaSequence(id, sequence, 'SetNode');
      }
    } else if (val instanceof NomsMap) {
      const sequence = val.sequence;
      if (sequence instanceof MapLeafSequence) {
        data.nodes[id] = {name: `Map (${val.size})`};
        sequence.items.forEach(entry => {
          const k = entry.key;
          const v = entry.value;
          // TODO: handle non-string keys
          const kid = process(ref, k, id);
          if (kid) {
            data.nodes[kid].isOpen = true;

            process(ref, v, kid);
          } else {
            throw new Error('No kid id.');
          }
        });
      } else {
        invariant(sequence instanceof OrderedMetaSequence);
        processMetaSequence(id, sequence, 'MapNode');
      }
    } else if (val instanceof Ref) {
      const refStr = val.toString();
      data.nodes[id] = {
        canOpen: true,
        name: refStr.substr(5, 6),
        ref: val,
      };
    } else if (val instanceof Struct) {
      // Struct
      // Make a variable to the struct to work around Flow bug.
      const struct: Struct = val;
      const structName = val.typeDef.name;
      data.nodes[id] = {name: structName};

      const processField = (k: string) => {
        const v = struct.get(k);
        const kid = process(ref, k, id);
        if (kid) {
          // Start struct keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, v, kid);
        } else {
          throw new Error('No kid id.');
        }
      };

      val.typeDef.desc.fields.forEach(f => {
        processField(f.name);
      });
      if (val.hasUnion) {
        const {name} = val.typeDef.desc.union[val.unionIndex];
        processField(name);
      }
    } else {
      console.log('Unsupported type', val.constructor.name, val); // eslint-disable-line no-console
    }

    return id;
  }

  process(ref, val, fromRef);
  render();
}

function handleNodeClick(e: MouseEvent, id: string) {
  if (e.button === 0 && !e.shiftKey && !e.ctrlKey && !e.altKey && !e.metaKey) {
    e.preventDefault();
  }

  if (id.indexOf('/') > -1) {
    if (data.links[id] && data.links[id].length > 0) {
      data.nodes[id].isOpen = !Boolean(data.nodes[id].isOpen);
      render();
    }
  } else {
    data.nodes[id].isOpen = !Boolean(data.nodes[id].isOpen);
    if (data.links[id] || !data.nodes[id].isOpen) {
      render();
    } else {
      const ref = Ref.parse(id);
      dataStore.readValue(ref).then(value => {
        handleChunkLoad(ref, value, id);
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
        Can haz datastore?
        <form style={{margin:'0.5em 0'}} onSubmit={() => this._handleOnSubmit()}>
          <input type='text' ref='store' autoFocus={true} style={inputStyle}
            defaultValue={params.store || 'http://api.noms.io/-/ds/[user]'}
            placeholder='noms store URL'
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

  _handleOnSubmit() {
    const {store, token, ref} = this.refs;
    let params = 'store=' + store.value;
    if (token.value) {
      params += '&token=' + token.value;
    }
    if (ref.value) {
      params += '&ref=' + ref.value;
    }
    window.location.pushState({}, undefined, '?' + params);
    render();
  }
}

function renderPrompt() {
  ReactDOM.render(<Prompt/>, renderNode);
}

function render() {
  const dt = new TreeNode(data, rootRef.toString(), null, 0, 0, {});
  layout(dt);
  ReactDOM.render(
    <Layout tree={dt} data={data} onNodeClick={handleNodeClick} nomsStore={params.store}/>,
    renderNode);
}
