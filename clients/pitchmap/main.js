var React = require('react');
var Immutable = require('immutable');
var store = require('./noms_store.js');
var decode = require('./decode.js');
var Map = React.createFactory(require('./map.js'));

function createTestData() {
  var set = Immutable.Set();
  for (var i = 0; i < 200; i++) {
    set = set.add(Immutable.Map({
      x: Math.random(),
      y: Math.random(),
    }));
  }
  return set;
}

store.getRoot().then(function(root) {
  var testData = createTestData();
  decode.readValue(root, store.getChunk).then(function(value) {
    var target = document.getElementById('heatmap');
    React.render(Map({
      points: testData,
      width: 287,
      height: 330,
    }), target);
  });
});
