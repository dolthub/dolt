/* @flow */

'use strict';

import {layout, NodeGraph, TreeNode} from './buchheim.js';
import Layout from './layout.js';
import React from 'react'; //eslint-disable-line no-unused-vars
import ReactDOM from 'react-dom';
import {readValue, HttpStore, Ref} from 'noms';

let data: NodeGraph = {nodes: {}, links: {}};
let rootRef: Ref;
let httpStore: HttpStore;
let renderNode: ?HTMLElement;

window.addEventListener('load', () => {
  renderNode = document.getElementById('splore');
  httpStore = new HttpStore('http://localhost:8000');

  httpStore.getRoot().then(ref => {
    rootRef = ref;
    handleChunkLoad(ref, ref);
  });
});

window.onresize = render;

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
      id = ref + '/' + counter++;
    }

    // Populate links.
    if (fromId) {
      (data.links[fromId] || (data.links[fromId] = [])).push(id);
    }

    switch (typeof val) {
      case 'bool':
      case 'number':
      case 'string':
        data.nodes[id] = {name: String(val)};
        break;
    }

    if (val instanceof Blob) {
      data.nodes[id] = {name: `Blob (${val.size})`};
    } else if (Array.isArray(val)) {
      data.nodes[id] = {name: `List (${val.length})`};
      val.forEach(c => process(ref, c, id));
    } else if (val instanceof Set) {
      data.nodes[id] = {name: `Set (${val.size})`};
      val.forEach(c => process(ref, c, id));
    } else if (val instanceof Map) {
      data.nodes[id] = {name: `Map (${val.size})`};
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
    } else if (val instanceof Ref) {
      let refStr = val.toString();
      data.nodes[id] = {
        canOpen: true,
        name: refStr.substr(5, 6),
        fullName: refStr
      };
    } else if (val._typeRef) {
      // Struct
      let structName = val._typeRef.name;
      data.nodes[id] = {name: structName};
      Object.keys(val).forEach(k => {
        if (k === '_typeRef') {
          return;
        }
        // TODO: handle non-string keys
        let kid = process(ref, k, id);
        if (kid) {
          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, val[k], kid);
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
      readValue(Ref.parse(id), httpStore).then(value => {
        handleChunkLoad(id, value, id);
      });
    }
  }
}

function render() {
  let dt = new TreeNode(data, rootRef.toString(), null, 0, 0, {});
  layout(dt);
  ReactDOM.render(<Layout tree={dt} data={data} onNodeClick={handleNodeClick}/>, renderNode);
}
