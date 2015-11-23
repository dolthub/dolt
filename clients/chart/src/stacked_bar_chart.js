/* @flow */

import d3 from './d3.js';
import nv from 'nvd3';
import React from 'react'; //eslint-disable-line no-unused-lets
import ReactDOM from 'react-dom';
import type {DataArray} from './data.js';

type Props = {
  data: DataArray
};

type State = {
  chart: ?Object
};

type DefaultProps = {};

export default class StackedBarChart extends React.Component<DefaultProps, Props, State> {
  componentDidMount() {
    nv.addGraph(() => {
      let chart = nv.models.multiBarChart();
      chart.options({
        stacked: true,
        showControls: false,
        showLegend: false
      });

      let div = ReactDOM.findDOMNode(this);
      d3.select(div.firstChild).datum(this.props.data).call(chart);
      nv.utils.windowResize(() => chart.update());
      this.setState({chart});
    });
  }

  componentDidUpdate() {
    let chart = this.state.chart;
    if (chart) {
      let svg = ReactDOM.findDOMNode(this);
      d3.select(svg).datum(this.props.data).call(chart);
    }
  }

  render() : React.Element {
    return <svg/>;
  }
}
