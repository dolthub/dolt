'use strict';

var noms = require('noms');
var React = require('react');
var Root = require('./root.js');

noms.getRoot().then((rootRef) => {
  noms.readValue(rootRef, noms.getChunk).then(render);
});

function render(rootValue) {
  var target = document.getElementById('root');
  React.render(<Root rootValue={rootValue}/>, target);
}
