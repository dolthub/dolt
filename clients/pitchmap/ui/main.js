'use strict';

var noms = require('noms');
var React = require('react');
var Map = require('./map.js');

noms.getDataset('mlb/heatmap')
  .then(getPitchers)
  .then(renderPitchersList);

function getPitchers(datasetRoot) {
  return datasetRoot.deref().then((root) => {
    return root.first().deref();
  }).then((map) =>{
    return map.get('value').deref();
  });
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
