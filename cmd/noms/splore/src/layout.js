// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React, {Element} from 'react';
import {TreeNode} from './buchheim.js';
import Node from './node.js';
import type {NodeGraph} from './types.js';

type Props = {
  data: NodeGraph,
  onNodeClick: (e: MouseEvent, s: string) => any,
  tree: TreeNode,
};

export default function Layout(props: Props): Element<any> {
  const {data, onNodeClick, tree} = props;

  const layoutChildren = [];
  const edges = [];
  const keyEdges = [];
  const lookup = {};

  const spaceX = 100;
  const spaceY = 20;
  const paddingRight = 250;
  const getX = d => d.y * spaceX;
  const getY = d => d.x * spaceY;
  let maxX = 0;
  let minY = 0;
  let maxY = 0;

  const process = (treeNode: TreeNode, fromX: number, fromY: number) => {
    const {children, id, isOpen, node} = treeNode;
    const x = getX(treeNode);
    const y = getY(treeNode);

    maxX = Math.max(x + spaceX, maxX);
    minY = Math.min(y, minY);
    maxY = Math.max(y + spaceY, maxY);

    const n = (
      <Node
        fromX={fromX}
        fromY={fromY}
        hasChildren={node.hasChildren}
        isOpen={isOpen}
        key={'node$' + id}
        onClick={e => onNodeClick(e, id)}
        spaceX={spaceX}
        text={node.name}
        x={x}
        y={y}
      />
    );

    layoutChildren.push(n);
    lookup[id] = treeNode;

    children.forEach(c => {
      process(c, x, y);
    });

    // Only show links if the tree is open.
    if (isOpen) {
      data.links[id].forEach(lid => {
        edges.push([id, lid]);
      });
    }

    // Always show key links (key -> value and label -> value) even if the tree
    // is closed.
    data.keyLinks[id].forEach(lid => {
      keyEdges.push([id, lid]);
    });
  };

  process(tree, 0, 0, true);

  const edgeStyle = {
    stroke: '#ccc',
    strokeWidth: '1.5px',
  };

  edges.forEach(e => {
    const from = lookup[e[0]];
    const to = lookup[e[1]];
    layoutChildren.push(
      <path
        key={'edge$' + e[0] + '-' + e[1]}
        style={edgeStyle}
        d={`M${getX(from)},${getY(from)}L${getX(to)},${getY(to)}`}
      />,
    );
  });

  const keyEdgeStyle = {
    ...edgeStyle,
    stroke: 'steelblue',
  };

  keyEdges.forEach(e => {
    const from = lookup[e[0]];
    const to = lookup[e[1]];
    layoutChildren.push(
      <path
        key={'keyEdge$' + e[0] + '-' + e[1]}
        style={keyEdgeStyle}
        d={`M${getX(from)},${getY(from)}L${getX(to)},${getY(to)}`}
      />,
    );
  });

  const sortOrder = elm => (elm.type === 'path' ? 0 : 1);
  layoutChildren.sort((a, b) => sortOrder(a) - sortOrder(b));

  let translateY = spaceY;
  if (minY < 0) {
    translateY -= minY;
    maxY -= minY;
  }

  return (
    <svg width={maxX + spaceX + paddingRight} height={maxY + spaceY}>
      <g transform={`translate(${spaceX}, ${translateY})`}>
        {layoutChildren}
      </g>
    </svg>
  );
}
