// @flow

import {layout, NodeGraph, TreeNode} from './buchheim.js';
import Layout from './layout.js';
import React from 'react'; // eslint-disable-line no-unused-vars
import ReactDOM from 'react-dom';
import {CompoundList, CompoundMap, CompoundSet, ListLeaf, MapLeaf, readValue, SetLeaf, HttpStore, Ref, Struct} from 'noms';

let data: NodeGraph = {nodes: {}, links: {}};
let rootRef: Ref;
let httpStore: HttpStore;
let renderNode: ?HTMLElement;

const nomsServer: ?string = process.env.NOMS_SERVER;
if (!nomsServer) {
  throw new Error('NOMS_SERVER not set');
}

window.addEventListener('load', () => {
  renderNode = document.getElementById('splore');
  httpStore = new HttpStore(nomsServer);

  httpStore.getRoot().then(ref => {
    rootRef = ref;
    handleChunkLoad(ref, ref);
  });
});

window.onresize = render;

function formatKeyString(v: any): string {
  if (v instanceof Ref) {
    return v.toString().substring(5, 11);
  }

  return String(v);
}

function handleChunkLoad(ref: Ref, val: any, fromRef: ?string) {
  let counter = 0;

  function process(ref, val, fromId): ?string {
    if (typeof val === 'undefined') {
      return null;
    }

    // Assign a unique ID to this node.
    // We don't use the noms ref because we only want to represent values as shared in the graph if they are actually in the same chunk.
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
    } else if (val instanceof ListLeaf) {
      data.nodes[id] = {name: `List (${val.length})`};
      val.items.forEach(c => process(ref, c, id));
    } else if (val instanceof SetLeaf) {
      data.nodes[id] = {name: `Set (${val.size})`};
      val.items.forEach(c => process(ref, c, id));
    } else if (val instanceof MapLeaf) {
      data.nodes[id] = {name: `Map (${val.size})`};
      val.items.forEach(entry => {
        let k = entry.key;
        let v = entry.value;
        // TODO: handle non-string keys
        let kid = process(ref, k, id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, v, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
    } else if (val instanceof Ref) {
      let refStr = val.toString();
      data.nodes[id] = {
        canOpen: true,
        name: refStr.substr(5, 6),
        fullName: refStr
      };
    } else if (val instanceof Struct) {
      // Struct
      let structName = val.typeDef.name;
      data.nodes[id] = {name: structName};

      val.forEach((v, k) => {
        // TODO: handle non-string keys
        let kid = process(ref, k, id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, v, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
    } else if (val instanceof CompoundList) {
      data.nodes[id] = {name: 'ListNode'};
      val.items.forEach(tuple => {
        let kid = process(ref, formatKeyString(tuple.value), id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, tuple.ref, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
    } else if (val instanceof CompoundMap) {
      data.nodes[id] = {name: 'MapNode'};
      val.items.forEach(tuple => {
        let kid = process(ref, formatKeyString(tuple.value), id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, tuple.ref, kid);
        } else {
          throw new Error('No kid id.');
        }
      });
    } else if (val instanceof CompoundSet) {
      data.nodes[id] = {name: 'SetNode'};
      val.items.forEach(tuple => {
        let kid = process(ref, formatKeyString(tuple.value), id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, tuple.ref, kid);
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

function handleNodeClick(e: Event, id: string) {
  if (e.altKey) {
    if (data.nodes[id].fullName) {
      window.prompt('Full ref', data.nodes[id].fullName);
    }
    return;
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
      let ref = Ref.parse(id);
      readValue(ref, httpStore).then(value => {
        handleChunkLoad(ref, value, id);
      });
    }
  }
}

function render() {
  let dt = new TreeNode(data, rootRef.toString(), null, 0, 0, {});
  layout(dt);
  ReactDOM.render(<Layout tree={dt} data={data} onNodeClick={handleNodeClick}/>, renderNode);
}
