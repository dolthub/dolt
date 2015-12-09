/* @flow */

import HeatMap from './heat_map.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {readValue, HttpStore, Ref} from 'noms';

let httpStore: HttpStore;

window.addEventListener('load', async () => {
  httpStore = new HttpStore('http://localhost:8000');
  let rootRef = await httpStore.getRoot();
  let datasets = await readValue(rootRef, httpStore);
  let commitRef = datasets.get('mlb/heatmap');
  let commit = await readValue(commitRef, httpStore);
  let pitchersMap = commit.get('value');
  renderPitchersMap(pitchersMap);
});

type Props = {
  pitchersMap: Map<string, Ref>
};

type State = {
  currentPitcher: string,
  pitchers: Array<string>
};

class PitcherList extends React.Component<void, Props, State> {
  constructor(props) {
    super(props);

    let pitchers = [];
    this.props.pitchersMap.forEach((ref, pitcher) => {
      pitchers.push(pitcher);
    });
    pitchers.sort();

    this.state = {
      currentPitcher: pitchers[0],
      pitchers: pitchers
    };
  }

  render() : React.Element {
    let currentPitcher = this.state.currentPitcher;
    let pitchListRef = this.props.pitchersMap.get(currentPitcher);

    let onChangePitcher = e => {
      this.setState({
        currentPitcher: e.target.value,
        pitchers: this.state.pitchers
      });
    };

    return <div>
      <select onChange={onChangePitcher} defaultValue={currentPitcher}>{
        this.state.pitchers.map(pitcher => {
          return <option key={pitcher} value={pitcher}>{pitcher}</option>;
        })
      }</select>
      <HeatMap key={currentPitcher} pitchListRef={pitchListRef} httpStore={httpStore}/>
    </div>;
  }
}

function renderPitchersMap(map: Map) {
  let renderNode = document.getElementById('heatmap');
  ReactDOM.render(<PitcherList pitchersMap={map}/>, renderNode);
}
