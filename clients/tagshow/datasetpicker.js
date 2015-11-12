/* @flow */

'use strict';

import {invariant} from './assert.js';
import {readValue} from 'noms';
import React from 'react';
import type {ChunkStore} from 'noms';

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
  constructor(props: Props) {
    super(props);
    this.state = {
      datasets: new Set()
    };
  }

  handleSelectChange(e: Event) {
    invariant(e.target instanceof HTMLSelectElement);
    this.props.onChange(e.target.value);
  }

  async _updateDatasets(props: Props) : Promise<void> {
    let store = props.store;
    let rootRef = await store.getRoot();
    let datasets = await readValue(rootRef, store);
    this.setState({
      datasets: new Set(datasets.keys())
    });
  }

  componentWillMount() {
    this._updateDatasets(this.props);
  }

  componentWillReceiveProps(props: Props) {
    this._updateDatasets(props);
  }

  render() : React.Element {
    let options = [];
    for (let v of this.state.datasets) {
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
