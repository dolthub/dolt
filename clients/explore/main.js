'use strict';

var React = require('react');
var TreeNode = React.createFactory(require('./tree_node.js'));
var store = require('./noms_store.js');
var decode = require('./decode.js');

store.getRoot().then(function(root) {
  decode.readValue(root, store.getChunk).then(function(value) {
    var target = document.getElementById('explore');

    React.render(
      TreeNode({ name: "root: " + root, value: value }), target
    );
  });
});
