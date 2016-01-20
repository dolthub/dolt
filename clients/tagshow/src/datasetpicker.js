// @flow

import eq from './eq.js';
import React from 'react';
import type {ChunkStore} from 'noms';
import {invariant, NomsMap, readValue, Ref} from 'noms';

type DefaultProps = {
  selected: string
};

type Props = {
  store: ChunkStore,
  onChange: (value: string) => void,
  selected: string
};

type State = {
  datasets: Set<string>
};

export default class DatasetPicker extends React.Component<DefaultProps, Props, State> {
  static defaultProps: DefaultProps;

  constructor(props: Props) {
    super(props);
    this.state = {
      datasets: new Set(),
    };
  }

  handleSelectChange(e: Event) {
    invariant(e.target instanceof HTMLSelectElement);
    this.props.onChange(e.target.value);
  }

  async _updateDatasets(props: Props) : Promise<void> {
    const store = props.store;
    const rootRef = await store.getRoot();
    const datasets: NomsMap<string, Ref> = await readValue(rootRef, store);
    invariant(datasets);
    const s = new Set();
    await datasets.forEach((v, k) => {
      s.add(k);
    });
    this.setState({
      datasets: s,
    });
  }

  shouldComponentUpdate(nextProps: Props, nextState: State) : boolean {
    return !eq(nextProps, this.props) || !eq(nextState, this.state);
  }

  render() : React.Element {
    this._updateDatasets(this.props);

    const options = [];
    for (const v of this.state.datasets) {
      options.push(<option value={v} key={v}>{v}</option>);
    }
    return <form>
      Choose dataset:
      <br/>
      <select value={this.props.selected}
          onChange={e => this.handleSelectChange(e)}>
        <option/>
        {options}
      </select>
    </form>;
  }
}

DatasetPicker.defaultProps = {selected: ''};
