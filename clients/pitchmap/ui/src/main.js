// @flow

import HeatMap from './heat_map.js';
import React from 'react';
import ReactDOM from 'react-dom';
import {DataStore, HttpStore, invariant, NomsMap, Ref} from 'noms';

let datastore: DataStore;

const nomsServer: ?string = process.env.NOMS_SERVER;
if (!nomsServer) {
  throw new Error('NOMS_SERVER not set');
}
const datasetId: ?string = process.env.NOMS_DATASET_ID;
if (!datasetId) {
  throw new Error('NOMS_DATASET_ID not set');
}

window.addEventListener('load', async () => {
  datastore = new DataStore(new HttpStore(nomsServer));
  const head = await datastore.head(datasetId);
  invariant(head);
  const pitchersMap: NomsMap<string, Ref> = head.get('value');
  invariant(pitchersMap);
  const pitchers = [];
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
  state: State;

  constructor(props) {
    super(props);

    this.state = {
      currentPitcher: props.pitchers[0],
    };
  }

  render() : React.Element {
    const currentPitcher = this.state.currentPitcher;
    const pitchListRefP = this.props.pitchersMap.get(currentPitcher);

    const onChangePitcher = e => {
      this.setState({
        currentPitcher: e.target.value,
      });
    };

    return <div>
      <select onChange={onChangePitcher} defaultValue={currentPitcher}>{
        this.props.pitchers.map(pitcher => <option key={pitcher} value={pitcher}>{pitcher}</option>)
      }</select>
      <HeatMap key={currentPitcher} pitchListRefP={pitchListRefP} chunkStore={datastore}/>
    </div>;
  }
}

function renderPitchersMap(map: NomsMap<string, Ref>, pitchers: Array<string>) {
  const renderNode = document.getElementById('heatmap');
  ReactDOM.render(<PitcherList pitchersMap={map} pitchers={pitchers}/>, renderNode);
}
