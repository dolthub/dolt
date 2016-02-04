// @flow

import Layout from './layout.js';
import React from 'react'; // eslint-disable-line no-unused-vars
import ReactDOM from 'react-dom';
import {HttpStore, invariant, IndexedMetaSequence, ListLeafSequence, MapLeafSequence,
    OrderedMetaSequence, NomsList, NomsMap, NomsSet, readValue, Ref, SetLeafSequence, Struct,}
    from 'noms';
import {layout, NodeGraph, TreeNode} from './buchheim.js';

const data: NodeGraph = {nodes: {}, links: {}};
let rootRef: Ref;
let httpStore: HttpStore;
let renderNode: ?HTMLElement;

const hash = {};

window.onload = load;
window.onhashchange = load;
window.onresize = render;

function load() {
  renderNode = document.getElementById('splore');
  location.hash.substr(1).split('&').forEach(pair => {
    const [k, v] = pair.split('=');
    hash[k] = v;
  });

  if (!hash.server) {
    renderPrompt();
    return;
  }

  httpStore = new HttpStore(hash.server);

  httpStore.getRoot().then(ref => {
    rootRef = ref;
    handleChunkLoad(ref, ref);
  });
}

function formatKeyString(v: any): string {
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
    if (typeof val === 'undefined') {
      return null;
    }

    // Assign a unique ID to this node.
    // We don't use the noms ref because we only want to represent values as shared in the graph if
    // they are actually in the same chunk.
    let id;
    if (val instanceof Ref) {
      id = val.toString();
    } else {
      id = ref.toString() + '/' + counter++;
    }

    // Populate links.
    if (fromId) {
      (data.links[fromId] || (data.links[fromId] = [])).push(id);
    }

    switch (typeof val) {
      case 'boolean':
      case 'number':
      case 'string':
        data.nodes[id] = {name: String(val)};
        break;
    }

    if (val instanceof Blob) {
      data.nodes[id] = {name: `Blob (${val.size})`};
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
      const structName = val.typeDef.name;
      data.nodes[id] = {name: structName};

      val.forEach((v, k) => {
        // TODO: handle non-string keys
        const kid = process(ref, k, id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, v, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
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
      readValue(ref, httpStore).then(value => {
        handleChunkLoad(ref, value, id);
      });
    }
  }
}

function setServer(url: string) {
  location.hash = `server=${url}`;
}

type PromptState = {
  server: string,
}

class Prompt extends React.Component<void, {}, PromptState> {
  state: PromptState;

  render() {
    const fontStyle = {
      fontFamily: 'Menlo',
      fontSize: 14,
      width: '',
    };
    return <div style={{display: 'flex', height: '100%', alignItems: 'center',
      justifyContent: 'center'}}>
      <div style={fontStyle}>
        <label>Can haz server?
          <div style={{margin:'0.5em 0'}}>
            <input type='text' ref='input' autoFocus={true}
              style={Object.assign(fontStyle, {}, {width: '50ex'})}
              defaultValue='http://api.noms.io/-/ds/[user]'/>
          </div>
        </label>
        <div><button onClick={() => setServer(this.refs.input.value)}>OK</button></div>
      </div>
    </div>;
  }
}

function renderPrompt() {
  ReactDOM.render(<Prompt/>, renderNode);
}

function render() {
  const dt = new TreeNode(data, rootRef.toString(), null, 0, 0, {});
  layout(dt);
  ReactDOM.render(<Layout tree={dt} data={data} onNodeClick={handleNodeClick}/>, renderNode);
}
