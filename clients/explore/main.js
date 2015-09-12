'use strict';

var queryString = require('query-string');
var React = require('react');
var Root = require('./root.js');
var noms = require('noms');

var qs = queryString.parse(location.search);
if (qs.server) {
  noms.setServer(qs.server);
}

noms.getRoot().then((rootRef) => {
  noms.readValue(rootRef, noms.getChunk).then(render);
});

function render(rootValue) {
  var target = document.getElementById('explore');
  React.render(Root({
    name: 'Root',
    rootValue: rootValue,
  }), target);
}
