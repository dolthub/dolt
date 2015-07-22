'use strict';

var React = require('react');
var TreeNode = React.createFactory(require('./tree_node.js'));
var noms = require('noms')

noms.getRoot().then((rootRef) => {
  noms.readValue(rootRef, noms.getChunk).then(render);
});

function render(rootValue) {
  var target = document.getElementById('explore');
  React.render(TreeNode({ name: 'Root', value: rootValue }), target);
}


