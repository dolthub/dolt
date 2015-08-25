'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var DatasetPicker = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    datasets: React.PropTypes.instanceOf(Promise).isRequired,
    onChange: React.PropTypes.func.isRequired,
    selected: React.PropTypes.string,
  },

  getInitialState: function() {
    return {
      datasets: Immutable.Set(),
    };
  },

  handleSelectChange: function(e) {
    this.props.onChange(e.target.value);
  },

  render: function() {
    this.props.datasets.then(
      datasets => {
        this.setState({
          datasets: Immutable.Set.of(...datasets)
        });
      }
    );

    return <form>
      Dataset:
      <select value={this.props.selected}
          onChange={this.handleSelectChange}>
        <option/>
        { 
          this.state.datasets.map((v, k) => {
            return <option value={v.get('id')}>{v.get('id')}</option>
          }).toArray()
        }
      </select>
    </form>
  },
});

module.exports = React.createFactory(DatasetPicker);
