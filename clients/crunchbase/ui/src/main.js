/* @flow */

import getAllData from './data.js';
import RadioList from './radio_list.js';
import React from 'react';
import ReactDOM from 'react-dom';
import SeriesList from './series_list.js';
import StackedBarChart from './stacked_bar_chart.js';
import type {DataArray, DataEntry} from './data.js';

type DefaultProps = {};

type Props = {
  data: DataArray,
  timeItems: Array<string>,
  categories: Array<string>
};

type State = {
  selectedSeries: Set<DataEntry>,
  selectedTimeItem: string,
  selectedCategoryItem: string
};

class Main extends React.Component<DefaultProps, Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      selectedSeries: new Set(props.data),
      selectedTimeItem: props.timeItems[0],
      selectedCategoryItem: props.categories[0]
    };
  }
  _filteredData() : DataArray {
    return this.props.data.filter(o => this.state.selectedSeries.has(o));
  }

  _selectedSeriesChanged(s: Set<DataEntry>) {
    this.setState({selectedSeries: s});
  }

  _selectedTimeChanged(s: string) {
    this.setState({selectedTimeItem: s});
  }

  _selectedCategoryChanged(s: string) {
    this.setState({selectedCategoryItem: s});
  }

  render() : React.Element {
    return <div className='app'>
      <div>
        <StackedBarChart className='app-chart' data={this._filteredData()}/>
        <div className='app-controls'>
          <SeriesList title='Series' data={this.props.data}
              selected={this.state.selectedSeries}
              onChange={s => this._selectedSeriesChanged(s)}/>
          <RadioList title='Time' items={this.props.timeItems}
              selected={this.state.selectedTimeItem}
              onChange={s => this._selectedTimeChanged(s)}/>
          <RadioList title='Categories' items={this.props.categories}
              selected={this.state.selectedCategoryItem}
              onChange={s => this._selectedCategoryChanged(s)}/>
        </div>
      </div>
    </div>;
  }
}

const data = getAllData();
const timeItems = ['Current Quarter', 'Current Year'];
const categories = ['Finance', 'Tech', 'Bio', 'Education'];

window.addEventListener('load', () => {
  ReactDOM.render(
      <Main data={data} timeItems={timeItems} categories={categories}/>,
      document.querySelector('#app'));
});
