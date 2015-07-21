'use strict';

var React = require('react');
var TreeNode = React.createFactory(require('./tree_node.js'));
var noms = require('noms')

noms.getRoot().then(function(root) {
  noms.readValue(root, noms.getChunk).then(function(value) {
    var target = document.getElementById('explore');

    React.render(
      TreeNode({ name: 'Root', value: value }), target
    );
  });
});
