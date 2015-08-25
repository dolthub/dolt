'use strict';

var Immutable = require('immutable');
var noms = require('noms');
var queryString = require('query-string');
var React = require('react');
var Root = require('./root.js');

window.onload =
    window.onhashchange = render;

function updateQuery(qs) {
  location.hash = queryString.stringify(qs.toObject());
}

function render() {
  var qs = queryString.parse(location.hash);
  var target = document.getElementById('root');

  // NOTE: This actually does a fetch, so if render() starts getting called
  // more frequently (e.g., in response to window resize), then this should
  // get moved someplace else.
  var rootValue = noms.getRoot().then(
    rootRef => noms.readValue(rootRef, noms.getChunk))

  React.render(
    <Root
      qs={Immutable.Map(qs)}
      rootValue={rootValue}
      updateQuery={updateQuery}/>, target);
}
