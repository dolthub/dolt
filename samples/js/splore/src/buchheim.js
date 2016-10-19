// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// JavaScript implementation of Christoph Buchheim, Michael Jünger, Sebastian Leipert's tree layout
// algorithm. See: http://dl.acm.org/citation.cfm?id=729576.
//
// Thanks also to Bill Mill for the explanation and Python sample code:
// http://billmill.org/pymag-trees/.

// TreeNode represents one node of the tree visualization.

import type {Hash} from '@attic/noms';

function assertNotNull<T>(v: ?T): T {
  if (v !== null && v !== undefined) {
    return v;
  }

  throw new Error('Non-null assertion failed');
}

export type NodeData = {
  name: string,
  isOpen?: boolean,
  canOpen?: boolean,
  hash?: Hash,
};

export type NodeGraph = {
  nodes: {[key: string]: NodeData},
  links: {[key: string]: Array<string>},
};

export class TreeNode {
  x: number;
  y: number;
  data: NodeData;
  id: string;
  children: Array<TreeNode>;
  parent: ?TreeNode;
  thread: ?TreeNode;
  offset: number;
  ancestor: TreeNode;
  change: number;
  shift: number;
  number: number;
  mod: number;

  constructor(graph: NodeGraph, id: string, parent: ?TreeNode, depth: number, number: number,
      seen: {[key: string]: boolean}) {
    seen[id] = true;
    this.x = -1;
    this.y = depth;
    this.data = graph.nodes[id];
    this.id = id;
    this.children = ((this.data.isOpen && graph.links[id]) || [])
      .filter(cid => !(cid in seen))
      .map((cid, i) => new TreeNode(graph, cid, this, depth + 1, i + 1, seen));
    this.parent = parent;
    this.thread = null;
    this.offset = 0;
    this.ancestor = this;
    this.change = 0;
    this.shift = 0;
    this.number = number;
    this.mod = 0;
  }

  left(): ?TreeNode {
    if (this.children.length > 0) {
      return this.children[0];
    }
    return this.thread;
  }

  right(): ?TreeNode {
    if (this.children.length > 0) {
      return this.children[this.children.length - 1];
    }
    return this.thread;
  }

  leftBrother(): ?TreeNode {
    let n = null;
    if (this.parent) {
      for (let i = 0; i < this.parent.children.length; i++) {
        const node = this.parent.children[i];
        if (node === this) {
          return n;
        } else {
          n = node;
        }
      }
    }
    return n;
  }

  getLeftMostSibling(): ?TreeNode {
    if (this.parent && this !== this.parent.children[0]) {
      return this.parent.children[0];
    } else {
      return null;
    }
  }
}

export function layout(tree: TreeNode): void{
  firstWalk(tree, 1);
  secondWalk(tree, 0, 0);
}

function firstWalk(v: TreeNode, distance: number): void {
  if (v.children.length === 0) {
    if (v.getLeftMostSibling()) {
      v.x = assertNotNull(v.leftBrother()).x + distance;
    } else {
      v.x = 0;
    }
  } else {
    let defaultAncestor = v.children[0];
    for (let i = 0; i < v.children.length; i++) {
      const w = v.children[i];
      firstWalk(w, distance);
      defaultAncestor = apportion(w, defaultAncestor, distance);
    }
    executeShifts(v);

    const midpoint = (v.children[0].x + v.children[v.children.length - 1].x) / 2;

    const w = v.leftBrother();
    if (w) {
      v.x = w.x + distance;
      v.mod = v.x - midpoint;
    } else {
      v.x = midpoint;
    }
  }
}

function apportion(v: TreeNode, defaultAncestor: TreeNode, distance: number): TreeNode {
  const w = v.leftBrother();
  if (w !== null) {
    let vir = v;
    let vor = v;
    let vil = assertNotNull(w);
    let vol = assertNotNull(v.getLeftMostSibling());
    let sir = v.mod;
    let sor = v.mod;
    let sil = vil.mod;
    let sol = vol.mod;
    while (vil.right() && vir.left()) {
      vil = assertNotNull(vil.right());
      vir = assertNotNull(vir.left());
      vol = assertNotNull(vol.left());
      vor = assertNotNull(vor.right());
      vor.ancestor = v;
      const shift = (vil.x + sil) - (vir.x + sir) + distance;
      if (shift > 0) {
        const a = ancestor(vil, v, defaultAncestor);
        moveSubtree(a, v, shift);
        sir = sir + shift;
        sor = sor + shift;
      }
      sil += vil.mod;
      sir += vir.mod;
      sol += vol.mod;
      sor += vor.mod;
    }
    if (vil.right() && !vor.right()) {
      vor.thread = vil.right();
      vor.mod += sil - sor;
    } else {
      if (vir.left() && !vol.left()) {
        vol.thread = vir.left();
        vol.mod += sir - sol;
      }
      defaultAncestor = v;
    }
  }
  return defaultAncestor;
}

function moveSubtree(wl: TreeNode, wr: TreeNode, shift: number): void {
  const subtrees = wr.number - wl.number;
  wr.change -= shift / subtrees;
  wr.shift += shift;
  wl.change += shift / subtrees;
  wr.x += shift;
  wr.mod += shift;
}

function executeShifts(v: TreeNode): void {
  let shift = 0;
  let change = 0;
  for (let i = v.children.length - 1; i >= 0; i--) {
    const w = v.children[i];
    w.x += shift;
    w.mod += shift;
    change += w.change;
    shift += w.shift + change;
  }
}

function ancestor(vil: TreeNode, v: TreeNode, defaultAncestor: TreeNode): TreeNode {
  if (v.parent && v.parent.children.indexOf(vil.ancestor) > -1) {
    return vil.ancestor;
  } else {
    return defaultAncestor;
  }
}

function secondWalk(v: TreeNode, m: number, depth: number): void {
  v.x += m;
  v.y = depth;

  for (let i = 0; i < v.children.length; i++) {
    secondWalk(v.children[i], m + v.mod, depth + 1);
  }
}
