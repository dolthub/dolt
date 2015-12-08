// @flow

import d3 from './d3.js';
import nv from 'nvd3';
import React from 'react';
import ReactDOM from 'react-dom';
import type {DataArray} from './data.js';

type Props = {
  data: DataArray,
  color?: Array<string>
};

type State = {
  chart: ?Object
};

type DefaultProps = {};

export default class Chart extends React.Component<DefaultProps, Props, State> {
  componentDidMount() {
    nv.addGraph(() => {
      let chart = nv.models.lineChart();
      chart.options({
        clipEdge: true,
        color: this.props.color,
        interpolate: 'basis',
        isArea: true,
        showControls: false,
        showLegend: false,
        useInteractiveGuideline: true
      });

      chart.yScale(d3.scale.log());
      let d3Format = d3.format('.2s');
      let format = v => '$' + d3Format(v).toUpperCase().replace('G', 'B');
      chart.yAxis.tickValues([1e4, 1e5, 1e6, 1e7, 1e8, 1e9]);
      chart.yAxis.tickFormat(format);
      chart.yDomain([1e3, 1e9]);
      chart.xAxis.tickFormat(v => (v * 100 | 0) + '%');

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

  render(): React.Element {
    return <svg/>;
  }
}
