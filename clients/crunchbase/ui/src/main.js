// @flow
import {DataStore, HttpStore} from '@attic/noms';
import Chart from './chart.js';
import DataManager from './data.js';
import List from './list.js';
import React from 'react';
import ReactDOM from 'react-dom';
import type {DataArray} from './data.js';
import type {ListDelegate} from './list.js';

type LabelAndKey = {
  label: string,
  key: Object,
};

type Props = {
  series: Array<string>,
  timeItems: Array<LabelAndKey>,
  categories: Array<string>,
  color: Array<string>,
};

type State = {
  selectedSeries: Set<string>,
  selectedTimeItem: LabelAndKey,
  selectedCategoryItem: string,
  data: DataArray,
};

const nomsServer: ?string = process.env.NOMS_SERVER;
if (!nomsServer) {
  throw new Error('NOMS_SERVER not set');
}
const datasetId: ?string = process.env.NOMS_DATASET_ID;
if (!datasetId) {
  throw new Error('NOMS_DATASET_ID not set');
}

class Main extends React.Component<void, Props, State> {
  state: State;
  _dataManager: DataManager;

  constructor(props: Props) {
    super(props);

    const selectedTimeItem = props.timeItems[0];
    const selectedCategoryItem = props.categories[0];

    const datastore = new DataStore(new HttpStore(nomsServer));
    this._dataManager = new DataManager(datastore, datasetId);

    this.state = {
      selectedSeries: new Set(this.props.series),
      selectedTimeItem,
      selectedCategoryItem,
      data: [],
    };
  }

  shouldComponentUpdate(nextProps: Props, nextState: State) : boolean {
    return nextProps !== this.props ||
        nextState.selectedSeries !== this.state.selectedSeries ||
        nextState.selectedTimeItem !== this.state.selectedTimeItem ||
        nextState.selectedCategoryItem !== this.state.selectedCategoryItem ||
        nextState.data !== this.state.data;
  }

  _filteredData(): DataArray {
    return this.state.data.filter(o => this.state.selectedSeries.has(o.key));
  }

  _selectedSeriesChanged(s: Set<string>) {
    this.setState({selectedSeries: s});
  }

  _selectedTimeChanged(item: LabelAndKey) {
    this.setState({selectedTimeItem: item});
  }

  _selectedCategoryChanged(s: string) {
    this.setState({selectedCategoryItem: s});
  }

  render(): React.Element {
    const s = this.state;
    const dm = this._dataManager;
    dm.getData(s.selectedTimeItem.key, s.selectedCategoryItem).then(data => {
      this.setState({data});
    }).catch(ex => {
      console.error(ex);  // eslint-disable-line
    });

    const seriesDelegate: ListDelegate<string> = {
      getLabel(item: string): string {
        return item;
      },
      isSelected: (item: string) => this.state.selectedSeries.has(item),
      getColor: (item: string) => this.props.color[this.props.series.indexOf(item)],
      onChange: (item: string) => {
        const selectedSeries = new Set(this.state.selectedSeries);
        if (selectedSeries.has(item)) {
          selectedSeries.delete(item);
        } else {
          selectedSeries.add(item);
        }
        this.setState({selectedSeries});
      },
    };

    const timeDelegate: ListDelegate<LabelAndKey> = {
      getLabel(item: LabelAndKey): string {
        return item.label;
      },
      isSelected: (item: LabelAndKey) => item === this.state.selectedTimeItem,
      onChange: (item: LabelAndKey) => {
        this._selectedTimeChanged(item);
      },
    };

    const categoryDelegate: ListDelegate<string> = {
      getLabel(item: string): string {
        return item;
      },
      isSelected: (item: string) => item === this.state.selectedCategoryItem,
      onChange: (item: string) => {
        this._selectedCategoryChanged(item);
      },
    };

    return <div className='app'>
      <div>
        <Chart className='app-chart' data={this._filteredData()}
            color={this.props.color}/>
        <div className='app-controls'>
          <List title='Series' items={this.props.series}
              delegate={seriesDelegate}/>
          <List title='Time' items={this.props.timeItems}
              delegate={timeDelegate}/>
          <List title='Categories' items={this.props.categories}
              delegate={categoryDelegate}/>
        </div>
      </div>
    </div>;
  }
}

const series = ['Seed', 'A', 'B'];

// Hardcode the time.
const year = 2015;
const qYear = 2105;
const quarter = 2;
const timeItems = [
  {label: '2015', key: {Year: year}},
  {label: '2015 Q2', key: {Year: qYear, Quarter: quarter}},
];

const categories = [
  'Biotechnology',
  'Finance',
  'Games',
  'Software',
];

const color = ['#011f4b', '#03396c', '#005b96'];

window.addEventListener('load', () => {
  ReactDOM.render(
      <Main series={series} timeItems={timeItems} categories={categories}
          color={color}/>,
      document.querySelector('#app'));
});
