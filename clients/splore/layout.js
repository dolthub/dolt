// @flow

import Node from './node.js';
import React from 'react';
import type {NodeGraph} from './buchheim.js';
import {TreeNode} from './buchheim.js';

type Props = {
  data: NodeGraph,
  onNodeClick: (e: Event, s: string) => void,
  tree: TreeNode
}

export default function Layout(props: Props) : React.Element {
  let children = [];
  let edges = [];
  let lookup = {};

  const spaceX = 75;
  const spaceY = 20;
  const paddingRight = 250;
  let getX = d => d.y * spaceX;
  let getY = d => d.x * spaceY;
  let maxX = 0;
  let minY = 0;
  let maxY = 0;

  let process = (treeNode, fromX, fromY) => {
    let links = props.data.links[treeNode.id] || [];
    let hasChildren = treeNode.data.canOpen || links.length > 0;
    let x = getX(treeNode);
    let y = getY(treeNode);
    let title = '';

    if (treeNode.data.fullName) {
      title = 'Alt-click for full ref';
    }

    maxX = Math.max(x + spaceX, maxX);
    minY = Math.min(y, minY);
    maxY = Math.max(y + spaceY, maxY);

    let n = (
      <Node
        key={'n' + treeNode.id}
        shape='circle'
        fromX={fromX}
        fromY={fromY}
        x={x}
        y={y}
        text={treeNode.data.name}
        title={title}
        canOpen={hasChildren}
        isOpen={!hasChildren || Boolean(treeNode.data.isOpen)}
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
    let from = lookup[e[0]];
    let to = lookup[e[1]];
    children.push(
      <path key={'p' + e[0] + '-' + e[1]} className='link' d={`M${getX(from)},${getY(from)}L${getX(to)},${getY(to)}`}/>);
  });

  let sortOrder = (elm => elm.type === 'path' ? 0 : 1);
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
