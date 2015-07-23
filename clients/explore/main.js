'use strict';

var React = require('react');
var Root = require('./root.js');
var noms = require('noms');

noms.getRoot().then((rootRef) => {
  noms.readValue(rootRef, noms.getChunk).then(render);
});

function render(rootValue) {
  var target = document.getElementById('explore');
  React.render(Root({ name: 'Root', rootValue: rootValue }), target);
}
