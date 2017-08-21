// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import {notNull} from './assert.js';
import {layout, TreeNode} from './buchheim.js';
import Layout from './layout.js';
import type {NodeGraph, SploreNode} from './types.js';

// The ID of the root node is always ''.
const rootId = '';

const data: NodeGraph = {
  keyLinks: {},
  links: {},
  nodes: {},
  open: {},
};

window.onload = window.onpopstate = load;
window.onresize = render;

async function load(): Promise<void> {
  await fetchNode(rootId);
  toggleNode(rootId);
}

async function fetchNode(id: string): Promise<void> {
  if (nodeIsFetched(id)) {
    throw new Error(`node ${id} has already been fetched`);
  }

  const idParam = encodeURIComponent(id);
  const node: SploreNode = await fetch(`/getNode?id=${idParam}`).then(r => r.json());
  addNode(node);

  const autoExpand = [];

  for (const child of notNull(node.children)) {
    const {key, label, value} = child;

    addNode(value);

    // Auto-expand @target because typically we'll only see these when
    // expanding a ref, and if a ref is being expanded, chances are they want
    // to see its target immediately.
    if (value.id.match(/@target$/)) {
      autoExpand.push(value.id);
    }

    if (label !== '') {
      // Includes a label. Create a fake node for it, then key-link to the
      // value. The @label annotation is just to trick the graph rendering.
      const labelNode = {
        hasChildren: false,
        id: value.id + '@label',
        name: label,
      };
      addNode(labelNode);
      data.links[id].push(labelNode.id);
      data.keyLinks[labelNode.id].push(value.id);
    } else if (key.id !== '') {
      // Includes a key. Add the node for it, then key-link to the value.
      // Note: unlike labels, keys can have their own subgraph.
      addNode(key);
      data.links[id].push(key.id);
      data.keyLinks[key.id].push(value.id);
    } else {
      // Only has a value.
      data.links[id].push(value.id);
    }
  }

  await Promise.all(autoExpand.map(fetchNode));
}

function nodeIsFetched(id: string): boolean {
  // The node might exist in data.nodes, but only as populated from the
  // children of another node. A node has only been fetched if it also has a
  // `children` property.
  const node = data.nodes[id];
  return !!(node && node.children);
}

function addNode(node: SploreNode): void {
  if (!nodeIsFetched(node.id)) {
    data.nodes[node.id] = node;
  }
  const init = arr => (arr[node.id] = arr[node.id] || []);
  init(data.links);
  init(data.keyLinks);
}

function handleNodeClick(e: MouseEvent, id: string) {
  if (e.shiftKey) {
    // TODO: Implement our own prompt which does pretty printing and allows copy.
    alert(JSON.stringify(data.nodes[id]));
  } else {
    toggleNode(id);
  }
}

async function toggleNode(id: string) {
  const node = data.nodes[id];
  if (!node || !node.hasChildren) {
    return;
  }

  if (!nodeIsFetched(id)) {
    await fetchNode(id);
  }

  data.open[id] = !data.open[id];
  render();
}

function render() {
  if (Object.keys(data).length === 0) {
    // Data hasn't loaded yet.
    return;
  }

  const dt = new TreeNode(data, rootId, null, 0, 0, {});
  layout(dt);
  ReactDOM.render(
    <Layout tree={dt} data={data} onNodeClick={handleNodeClick} />,
    document.querySelector('#splore'),
  );
}
