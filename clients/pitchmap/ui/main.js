'use strict';

var noms = require('noms');
var React = require('react');
var Map = require('./map.js');

noms.getRoot()
    .then(ref => {
      var pRoot = noms.readValue(ref, noms.getChunk);
      return noms.getDataset(pRoot, 'mlb/heatmap');
    })
    .then(getPitchers)
    .then(renderPitchersList);

function getPitchers(datasetRootRef) {
  return datasetRootRef.deref()
      .then(datasetRoot => datasetRoot.get('value').deref());
}

var PitcherList = React.createClass({
  getInitialState() {
    var pitchers = this.props.data.map((v, key) => key).toArray();
    pitchers.sort();

    return {
      currentPitcher: pitchers[0],
      pitchers: pitchers
    };
  },

  onChangePitcher(e) {
    this.setState({ currentPitcher: e.target.value });
  },

  render() {
    var currentPitcher = this.state.currentPitcher;
    var locations = this.props.data.get(currentPitcher);

    return <div>
      <select onChange={this.onChangePitcher} defaultValue={currentPitcher}>{
        this.state.pitchers.map((pitcher) => {
          var isCurrent = currentPitcher === pitcher;
          return <option key={pitcher} value={pitcher}>{pitcher}</option>
        })
      }</select>
      <Map key={currentPitcher} points={locations}/>
    </div>
  }
});

function renderPitchersList(list) {
  var target = document.getElementById('heatmap');
  React.render(<PitcherList data={list}/>, target);
}
