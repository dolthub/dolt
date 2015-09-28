'use strict';

var Node = require('./node.js');
var React = require('react');

var Layout = React.createClass({
  propTypes: {
    data: React.PropTypes.object.isRequired,
    onToggle: React.PropTypes.func.isRequired,
    tree: React.PropTypes.object.isRequired,
  },

  render() {
    var children = [];
    var edges = [];
    var lookup = {};

    const spaceX = 75;
    const spaceY = 20;
    var getX = d => d.y * spaceX;
    var getY = d => d.x * spaceY;
    var maxX = 0;
    var minY = 0;
    var maxY = 0;

    var process = (treeNode, fromX, fromY) => {
      var links = this.props.data.links[treeNode.id] || [];
      var hasChildren = treeNode.data.canOpen || links.length > 0;
      var x = getX(treeNode);
      var y = getY(treeNode);

      maxX = Math.max(x + spaceX, maxX);
      minY = Math.min(y, minY);
      maxY = Math.max(y + spaceY, maxY);

      var n = (
        <Node
          key={'n' + treeNode.id}
          shape='circle'
          fromX={fromX}
          fromY={fromY}
          x={x}
          y={y}
          text={treeNode.data.name}
          canOpen={hasChildren}
          isOpen={!hasChildren || Boolean(treeNode.data.isOpen)}
          onToggle={() => this.props.onToggle(treeNode.id)}/>);
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

    process(this.props.tree, 0, 0);

    edges.forEach(e => {
      var from = lookup[e[0]];
      var to = lookup[e[1]];
      children.push(
        <path key={'p' + e[0] + '-' + e[1]} className='link' d={`M${getX(from)},${getY(from)}L${getX(to)},${getY(to)}`}/>);
    });

    var sortOrder = (elm => elm.type == 'path' ? 0 : 1);
    children.sort((a, b) => sortOrder(a) - sortOrder(b));

    var translateY = spaceY;
    if (minY < 0) {
      translateY -= minY;
      maxY -= minY;
    }

    return (
      <svg width={maxX + spaceX} height={maxY + spaceY}>
        <g transform={`translate(${spaceX}, ${translateY})`}>
          {children}
        </g>
      </svg>
    );
  },
});

module.exports = React.createFactory(Layout);
