// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Node from './node.js';
import React from 'react';
import type {NodeGraph} from './buchheim.js';
import {TreeNode} from './buchheim.js';

type Props = {
  data: NodeGraph,
  onNodeClick: (e: MouseEvent, s: string) => void,
  tree: TreeNode,
  db: string,
};

export default function Layout(props: Props) : React.Element<any> {
  const children = [];
  const edges = [];
  const lookup = {};

  const spaceX = 100;
  const spaceY = 20;
  const paddingRight = 250;
  const getX = d => d.y * spaceX;
  const getY = d => d.x * spaceY;
  let maxX = 0;
  let minY = 0;
  let maxY = 0;

  const process = (treeNode, fromX, fromY) => {
    const links = props.data.links[treeNode.id] || [];
    const hasChildren = treeNode.data.canOpen || links.length > 0;
    const x = getX(treeNode);
    const y = getY(treeNode);
    const hash = treeNode.data.hash;
    const title = hash ? hash.toString() : '';

    maxX = Math.max(x + spaceX, maxX);
    minY = Math.min(y, minY);
    maxY = Math.max(y + spaceY, maxY);

    const n = (
      <Node
        key={'n' + treeNode.id}
        shape='circle'
        fromX={fromX}
        fromY={fromY}
        x={x}
        y={y}
        spaceX={spaceX}
        text={treeNode.data.name}
        title={title}
        canOpen={hasChildren}
        isOpen={!hasChildren || Boolean(treeNode.data.isOpen)}
        hash={hash}
        db={props.db}
        onClick={(e) => props.onNodeClick(e, treeNode.id)}/>);
    children.push(n);
    lookup[treeNode.id] = treeNode;

    if (treeNode.data.isOpen) {
      treeNode.children.forEach(c => {
        process(c, x, y);
      });
      links.forEach(l => {
        edges.push([treeNode.id, l]);
      });
    }
  };

  process(props.tree, 0, 0);

  edges.forEach(e => {
    const from = lookup[e[0]];
    const to = lookup[e[1]];
    children.push(
      <path key={'p' + e[0] + '-' + e[1]} className='link'
          d={`M${getX(from)},${getY(from)}L${getX(to)},${getY(to)}`}/>);
  });

  const sortOrder = (elm => elm.type === 'path' ? 0 : 1);
  children.sort((a, b) => sortOrder(a) - sortOrder(b));

  let translateY = spaceY;
  if (minY < 0) {
    translateY -= minY;
    maxY -= minY;
  }

  return (
    <svg width={maxX + spaceX + paddingRight} height={maxY + spaceY}>
      <g transform={`translate(${spaceX}, ${translateY})`}>
        {children}
      </g>
    </svg>
  );
}
