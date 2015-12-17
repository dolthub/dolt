// @flow

import HeatMap from './heat_map.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {HttpStore, invariant, NomsMap, readValue, Ref, Struct} from 'noms';

let httpStore: HttpStore;

const nomsServer: ?string = process.env.NOMS_SERVER;
if (!nomsServer) {
  throw new Error('NOMS_SERVER not set');
}
const datasetId: ?string = process.env.NOMS_DATASET_ID;
if (!datasetId) {
  throw new Error('NOMS_DATASET_ID not set');
}

window.addEventListener('load', async () => {
  httpStore = new HttpStore(nomsServer);
  let rootRef = await httpStore.getRoot();
  let datasets: NomsMap<string, Ref> = await readValue(rootRef, httpStore);
  let commitRef = await datasets.get(datasetId);
  invariant(commitRef);
  let commit:Struct = await readValue(commitRef, httpStore);
  let pitchersMap = commit.get('value');
  let pitchers = [];
  await pitchersMap.forEach((ref, pitcher) => {
    pitchers.push(pitcher);
  });

  pitchers.sort();
  renderPitchersMap(pitchersMap, pitchers);
});

type Props = {
  pitchersMap: NomsMap<string, Ref>,
  pitchers: Array<string>
};

type State = {
  currentPitcher: string
};

class PitcherList extends React.Component<void, Props, State> {
  constructor(props) {
    super(props);

    this.state = {
      currentPitcher: props.pitchers[0]
    };
  }

  render() : React.Element {
    let currentPitcher = this.state.currentPitcher;
    let pitchListRefP = this.props.pitchersMap.get(currentPitcher);

    let onChangePitcher = e => {
      this.setState({
        currentPitcher: e.target.value
      });
    };

    return <div>
      <select onChange={onChangePitcher} defaultValue={currentPitcher}>{
        this.props.pitchers.map(pitcher => {
          return <option key={pitcher} value={pitcher}>{pitcher}</option>;
        })
      }</select>
      <HeatMap key={currentPitcher} pitchListRefP={pitchListRefP} httpStore={httpStore}/>
    </div>;
  }
}

function renderPitchersMap(map: NomsMap<string, Ref>, pitchers: Array<string>) {
  let renderNode = document.getElementById('heatmap');
  ReactDOM.render(<PitcherList pitchersMap={map} pitchers={pitchers}/>, renderNode);
}
